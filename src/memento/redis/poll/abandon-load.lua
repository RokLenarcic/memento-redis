-- delete our load token if it's still there, otherwise leave it alone
if redis.call('get', _:k) == _:load-marker then
  redis.call('del', _:k)
  return _:k
end
return 0
