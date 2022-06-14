if redis.call('exists', _:id-key) then
  local ks = redis.call('smembers', _:id-key)
  if not (next(ks) == nil) then
    redis.call('del', unpack(ks))
    redis.call('del', _:id-key)
    redis.call('srem', _:indexes, _:id-key)
  end
end
