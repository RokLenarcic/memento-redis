local indexes = KEYS[1]
local epoch_key = KEYS[2]
local tag_epochs_key = KEYS[3]
local epoch = redis.call('incr', epoch_key)

for i = 4, #KEYS do
  local tagid = KEYS[i]
  redis.call('hset', tag_epochs_key, tagid, epoch)
  if redis.call('exists', tagid) then
    local ks = redis.call('smembers', tagid)
    if not (next(ks) == nil) then
      redis.call('del', unpack(ks))
      redis.call('del', tagid)
      redis.call('srem', indexes, tagid)
    end
  end
end
return epoch
