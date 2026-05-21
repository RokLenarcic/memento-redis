local load_marker = _:load-marker;
local fade = tonumber(_:fade-ms)
local ret = redis.call('get', _:k)

if ret then
  -- fast path, value is there
  if ret:sub(1, 43) ~= load_marker:sub(1, 43) and fade > 0 then
    -- if both ttl and fade are set, then fade cannot extend expire after write expiration
    -- fade must be smaller than ttl otherwise fade is useless, enforce this elsewhere though
    redis.call('pexpire', _:k, fade)
  end
  return {true, ret, 0}
else
  if _:load == "0" then
    return {false, ret, 0}
  else
    local validation_epoch = tonumber(redis.call('get', _:epoch-key) or '0')
    redis.call('set', _:k, load_marker, 'PX', tonumber(_:load-ms))
    return {false, load_marker, validation_epoch}
  end
end
