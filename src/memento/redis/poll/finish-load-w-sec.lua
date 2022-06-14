-- finish load with secondary indexes
-- keys are [k, indexes-key, id-key1, id-key2, ....]
-- values are a map
-- conditionally set our token
local ttl = tonumber(_:ttl-ms)
local k = KEYS[1]
local indexes = KEYS[2]
if '' == _:load-marker or redis.call('get', k) == _:load-marker then
  if ttl > 0 then
    redis.call('set', k, _:v, 'PX', _:ttl-ms)
  else
    redis.call('set', k, _:v)
  end
  for i = 3, #KEYS do
    redis.call('sadd', KEYS[i], k)
    redis.call('sadd', indexes, KEYS[i])
  end
end
