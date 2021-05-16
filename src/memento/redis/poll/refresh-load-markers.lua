local values = redis.call('mget', unpack(KEYS))
local fade_sec = tonumber(ARGV[#ARGV])
for i, v in ipairs(values) do
  if v == ARGV[i] then
    redis.call('expire', KEYS[i], fade_sec)
  end
end
