'''
This test can be run in Eclipse PyDev.
It uses the windows port of Redis (just for testing purposes) from our Amber Eclipse shared config.
The output is written by default to windows drive "E:\" , this is configurable in: "idetest_aof_read.json"
'''

import os
import time
import redis
import subprocess

from redisrollforward.redis_aof_read   import redis_aof_read  

# (re)start the redis db if running
try:
  cli = redis.StrictRedis('localhost', 45321)
  cli.shutdown()
  time.sleep(1.0)
except:
  pass

subprocess.Popen(
  [os.path.join(os.getenv('AmberEclipseWorkspace'), '.metadata/.plugins/org.eclipse.debug.core/.launches/tools/Redis-Msdn-2.6_alpha_x64/redis-server.exe'),
   os.path.join(os.getenv('AmberEclipseWorkspace'), 'py-redisrollforward/features/idetests/redis.conf')]) 

oRedisAofRead = redis_aof_read(
  os.path.join(os.getenv('AmberEclipseWorkspace'), 'py-redisrollforward/features/idetests/idetest_aof_read.json'), 
  bVerboseIP = True, 
  bLogToDefaultOutputIP=True) # Log to default output: useful for Eclipse IDE Console.
oRedisAofRead.ClientStart() 
oRedisAofRead.ClientStart() # When a second ClientStart is executed we expect a message that the client is already running  
oRedisAofRead.ClientStop()

time.sleep(5)
cli.shutdown()

print 'DONE'

#EOF
