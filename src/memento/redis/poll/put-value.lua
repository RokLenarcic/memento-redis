local ttl = tonumber(_:ttl-ms)
if ttl > 0 then
  redis.call('set', _:k, _:v, 'PX', _:ttl-ms)
else
  redis.call('set', _:k, _:v)
end
return 1;
