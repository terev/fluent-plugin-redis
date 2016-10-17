local exists = redis.call('EXISTS', ARGV[1])
if exists == 1 then
    for i = 1, #KEYS, 2 do
        redis.call('HINCRBY', ARGV[1], KEYS[i], KEYS[i + 1])
    end
    return "OK"
else
    redis.call('HMSET', ARGV[1], unpack(KEYS))
end
