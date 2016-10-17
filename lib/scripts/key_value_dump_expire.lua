local ttl = redis.call('TTL', KEYS[1])
if ttl > 0 then
    return redis.call('INCRBY', KEYS[1], ARGV[1])
else
    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')
end