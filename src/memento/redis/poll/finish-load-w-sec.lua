-- finish load with secondary indexes
-- keys are [k, indexes-key, epoch-key, tag-epochs-key, id-key1, id-key2, ....]
-- values are a map
local ttl = tonumber(_:ttl-ms)
local k = KEYS[1]
local indexes = KEYS[2]
local epoch_key = KEYS[3]
local tag_epochs_key = KEYS[4]

local function write_value_and_index()
  if ttl > 0 then
    redis.call('set', k, _:v, 'PX', _:ttl-ms)
  else
    redis.call('set', k, _:v)
  end
  for i = 5, #KEYS do
    redis.call('sadd', KEYS[i], k)
    redis.call('sadd', indexes, KEYS[i])
  end
end

if '' == _:load-marker then
  -- putValue path: authoritative bulk-insert by the user (addEntries).
  -- No load marker was placed in `k`, no fetch.lua claim happened, so there is
  -- nothing to clean up and nothing to validate against — the caller is asserting
  -- this is the current value as of now.
  write_value_and_index()
  return 1;
else
  -- Loader path: we placed a load marker in `k` via fetch.lua and captured the
  -- cache's validation_epoch at that moment. Only finalize if the load marker
  -- is still ours (no one invalidated/overwrote in between) AND no tag was
  -- invalidated since we started loading.
  if redis.call('get', k) ~= _:load-marker then
    return 0;
  end
  local validation_epoch = tonumber(_:validation-epoch)
  for i = 5, #KEYS do
    local tag_epoch = tonumber(redis.call('hget', tag_epochs_key, KEYS[i]) or '-1')
    if tag_epoch > validation_epoch then
      -- A tag we depend on was invalidated mid-load. Drop our load marker so
      -- joiners stop waiting on it.
      redis.call('del', k)
      return -1;
    end
  end
  write_value_and_index()
  return 1;
end
