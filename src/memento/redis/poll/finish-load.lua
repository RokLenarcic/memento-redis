-- conditionally set our token
local ttl = tonumber(_:ttl-ms)
if redis.call('get', _:k) == _:load-marker then
  if ttl > 0 then
    redis.call('set', _:k, _:v, 'PX', _:ttl-ms)
  else
    redis.call('set', _:k, _:v)
  end
  return 1;
end
return 0;
