
const RedisClient     = require('@kirick/redis-client/src/client');
// const { randomBytes } = require('crypto');
const OhMyProps       = require('oh-my-props');

const isPlainObject = (value) => value && typeof value === 'object' && value.constructor === Object;
const setTimeoutAsync = (time) => new Promise(resolve => {
    setTimeout(resolve, time);
});

const REDIS_KEY_PREFIX = 'locker:';

const optionsProps = new OhMyProps({
    ttl: {
        type: Number,
        is_nullable: true,
        default: null,
        validator: (value) => value > 0 && Number.isInteger(value),
    },
    try_delay: {
        type: Number,
        default: 25,
        validator: (value) => value > 0 && Number.isInteger(value),
    },
    try_limit: {
        type: Number,
        default: 10,
        validator: (value) => value > 0 && Number.isInteger(value),
    },
    onError: {
        type: Function,
        is_nullable: true,
        default: null,
    },
});

class RedisLocker {
    constructor (
        redisClient,
        options = {},
    ) {
        if (redisClient instanceof RedisClient !== true) {
            throw new TypeError('Argument 0 must be an instance of RedisClient.');
        }
        this.redisClient = redisClient;

        this.options = optionsProps.transform(options);
        if (null === this.options) {
            throw new TypeError('Argument 1 contains invalid options.');
        }

        this._keys_locked = new Set();
    }

    _spliceOptions (args) {
        if (isPlainObject(args[args.length - 1])) {
            return args.pop();
        }
    }

    _combineOptions (args) {
        const options_local = this._spliceOptions(args);
        if (options_local) {
            return optionsProps.transform({
                ...this.options,
                ...options_local,
            });
        }

        return this.options;
    }

    async lock (...keys) {
        const options = this._combineOptions(keys);
        if (null === this.options) {
            throw new TypeError('Invalid options found.');
        }

        // console.log('[REDIS-LOCKER] keys to lock', keys, 'with options', options);

        const {
            ttl,
            try_delay,
            try_limit,
            onError,
        } = options;

        const is_ttl_number = typeof ttl === 'number';

        let try_count = 1;

        // eslint-disable-next-line no-constant-condition
        while (1) {
            const multi = this.redisClient.MULTI();

            for (const key of keys) {
                const set_args = [
                    REDIS_KEY_PREFIX + key,
                    '',
                    'NX',
                ];

                if (is_ttl_number) {
                    set_args.push(
                        'PX',
                        ttl,
                    );
                }

                multi.SET(...set_args);
            }

            const response = await multi.EXEC();
            // console.log('[REDIS-LOCKER] response', response);

            const keys_set = new Set(keys);

            for (const [ index, value ] of response.entries()) {
                if ('OK' === value) {
                    const key = keys[index];

                    this._keys_locked.add(REDIS_KEY_PREFIX + key);
                    keys_set.delete(key);
                }
            }

            if (0 === keys_set.size) {
                break;
            }
            else {
                keys = Array.from(keys_set);

                if (try_count < try_limit) {
                    try_count++;
                    await setTimeoutAsync(try_delay);
                }
                else {
                    if (null !== onError) {
                        onError();
                    }

                    const error = new Error(`Cannot lock key "${keys[0]}".`);
                    error.code = 'LOCKED';
                    error.source = '@kirick/redis-locker';

                    throw error;
                }
            }
        }
    }

    /* async */ lockPattern (pattern, ...args) {
        const keys = new Set();

        const splitted = pattern.split('{}');

        if (typeof splitted[1] !== 'string' || splitted.length > 2) {
            throw new Error('Invalid pattern given.');
        }

        const options_local = this._spliceOptions(args);

        // console.log('[REDIS-LOCKER] patter values to lock:', args, 'with local options:', options_local);

        for (const value of args) {
            keys.add(splitted[0] + value + splitted[1]);
        }

        return this.lock(
            ...keys,
            options_local,
        );
    }

    async release () {
        const keys = this._keys_locked;
        if (keys.size > 0) {
            await this.redisClient.DEL(
                ...keys,
            );
        }
    }
}

module.exports = RedisLocker;
