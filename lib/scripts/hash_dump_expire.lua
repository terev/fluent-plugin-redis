local ttl = redis.call('TTL', ARGV[1])
if ttl > 0 then
    for i = 1, #KEYS, 2 do
        redis.call('HINCRBY', ARGV[1], KEYS[i], KEYS[i + 1])
    end
else
    redis.call('HMSET', ARGV[1], unpack(KEYS))
    redis.call('EXPIRE', ARGV[1], ARGV[2])
end