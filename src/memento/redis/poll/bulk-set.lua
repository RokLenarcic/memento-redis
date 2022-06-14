local cnt = #ARGV
local ttl = tonumber(ARGV[cnt])
for i, k in ipairs(KEYS) do
  if ttl > 0 then
    redis.call('set', k, ARGV[i], 'PX', ttl)
  else
    redis.call('set', k, ARGV[i])
  end
end
