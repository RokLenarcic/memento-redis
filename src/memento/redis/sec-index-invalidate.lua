if redis.call('exists', _:id-key) then
  redis.call('del', unpack(redis.call('smembers', _:id-key)))
  redis.call('del', _:id-key)
  redis.call('srem', _:indexes, _:id-key)
end
