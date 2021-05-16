-- special load marker that will be returned
-- when there is no value there, meaning that the expected
-- load token has expired without a foreign loader returning a value
-- this load token also does double duty as a type example
local load_marker = ARGV[1];

local gets = redis.call('mget', unpack(KEYS))
local ret = {}
for i, r in ipairs(gets) do
  if r then
    if r:sub(1, 43) ~= load_marker:sub(1, 43) then
      table.insert(ret, {KEYS[i], r})
    end
  else
    table.insert(ret, {KEYS[i], load_marker})
  end
end
return ret;
