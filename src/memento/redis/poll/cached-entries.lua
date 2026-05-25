-- KEYS layout: [k1, k2, ...]
-- For each input key, returns one of:
--   {i, 0}                          load marker still present (foreign loader has not written)
--   {i, 1, rawBytes}                value present (envelope bytes; may decode to nil)
--   {i, 2}                          key absent (foreign loader gave up / expired)
--
-- Status codes are integers so they survive Carmine's car/parse-raw cleanly.
-- We return the input *index* (1-based) rather than KEYS[i] so parse-raw does
-- not recursively rawify the key bytes; the Clojure caller maps i back to the
-- original key object it passed in.
local gets = redis.call('mget', unpack(KEYS))
local ret = {}
for i, r in ipairs(gets) do
  if r then
    if r:byte(1) == 0x03 then
      -- still a load marker (foreign loader has not written yet)
      table.insert(ret, {i, 0})
    else
      -- value envelope present
      table.insert(ret, {i, 1, r})
    end
  else
    -- key absent
    table.insert(ret, {i, 2})
  end
end
return ret;
