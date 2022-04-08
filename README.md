#redisrollforward: Daemon script, run together with redis-server, to collect AI blocks for roll-forwarding

##Use Case

For testing and troubleshooting purposes, a Redis developer wants to be able to instantiate a separate Redis instance, on a separate server, and apply a roll-forward up until a certain point in time.

This concept is similar to AI (After Imaging) in RDBMS databases.

This point in time could (for example) be: the previous day at 15:12:01.

The precision (per second) does not have to be exact. A few seconds earlier or a few seconds later is acceptable.

Incomplete transactions must be rolled back, or at least recognized.

Negative client performance impact on the live redis instance(s) should be kept to a minimum.

The AI chunks (copied from the AOF) should be forwarded as chunks of a configurable size, with a configurable pause/sleep in between. This is needed to spread the impact on TCP traffic, thus preventing negative concurrent performance impact.

##Implementation

This solution relies on the Redis AOF (Append Only File) mechanism. It requires a change in the default Redis configuration (redis.conf):

    Default:
    appendonly no
    auto-aof-rewrite-percentage 100
    auto-aof-rewrite-min-size 64mb
    
    Required:
    appendonly yes
    auto-aof-rewrite-percentage 0
    auto-aof-rewrite-min-size 0
    
You can also set this online from redis-cli:

    127.0.0.1:14130> config set auto-aof-rewrite-min-size 0
    127.0.0.1:14130> config set auto-aof-rewrite-percentage 0
    127.0.0.1:14130> config set appendonly yes
    
The `redisrollforward` script will do the following:

1. Do each second (configurable):
2. Write the current AOF file bytesize to a log
3. If the current AOF file bytesize is bigger than the previous one:
  - Write an AI chunk file

1. Do each night at midnight:
2. Stall all logging
3. Ensure all chunks of the current AOF are copied
4. Issue a `BGREWRITEAOF `
5. Check `INFO PERSISTENCE` until:
  - aof_rewrite_in_progress:0
  - aof_rewrite_scheduled:0  
6. Resume logging

## Requirements

- Redis
- yEd (flowcharts)

