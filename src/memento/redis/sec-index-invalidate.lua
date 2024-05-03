local indexes = KEYS[1]
for i = 2, #KEYS do
  local tagid = KEYS[i]
  if redis.call('exists', tagid) then
    local ks = redis.call('smembers', tagid)
    if not (next(ks) == nil) then
      redis.call('del', unpack(ks))
      redis.call('del', tagid)
      redis.call('srem', indexes, tagid)
    end
  end
end


