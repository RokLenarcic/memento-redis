-- explicit put with secondary indexes
-- keys are [k, indexes-key, epoch-key, tag-epochs-key, id-key1, id-key2, ....]
local ttl = tonumber(_:ttl-ms)
local k = KEYS[1]
local indexes = KEYS[2]

if ttl > 0 then
  redis.call('set', k, _:v, 'PX', _:ttl-ms)
else
  redis.call('set', k, _:v)
end

for i = 5, #KEYS do
  redis.call('sadd', KEYS[i], k)
  redis.call('sadd', indexes, KEYS[i])
end

return 1;
