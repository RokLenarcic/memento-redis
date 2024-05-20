local values = redis.call('mget', unpack(KEYS))
local fade_sec = tonumber(ARGV[#ARGV])
for i, v in ipairs(values) do
  if v == ARGV[i] then
    -- only refresh load marker if its ours
    redis.call('expire', KEYS[i], fade_sec)
  end
end
