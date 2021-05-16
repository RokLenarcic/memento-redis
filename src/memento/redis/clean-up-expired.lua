local keys = redis.call('srandmember', _:k, _:n)
if #keys == 0 then
    redis.call('srem', _:indexes, _:k)
    return {0,0}
else
    local vals = redis.call('mget', unpack(keys))
    local expired = {}
    for i, k in ipairs(keys) do
      if not vals[i] then
        table.insert(expired, k)
      end
    end
    if #expired ~= 0 then
      redis.call('srem', _:k, unpack(expired))
    end
    return {#keys, #expired}
end
