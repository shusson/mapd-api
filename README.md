# mapd-api

### what
Opens persistent session with mapd-core server, modifies all sql_execute requests with the persistent session then reverse proxies the 
requests to the mapd-core server.


### why
Enables load balancing without relying on sticky sessions

