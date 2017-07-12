# mapd-api

### Overview
The mapd-api does two particular things at the moment
 - Session presistence
 - Caching

### Session Persistence 
the api opens a session with mapd-core server and then modifies all incoming sql_execute requests with the session before reverse proxying the requests to the mapd-core server. This allows us to do load balancing without relying on sticky sessions.

### Caching
The api caches previous requests with a redis server. This allows us to dramatically improve the performance of common queries.
