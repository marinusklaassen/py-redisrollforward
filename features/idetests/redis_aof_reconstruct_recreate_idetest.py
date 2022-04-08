import time
import json
from redisrollforward.redis_aof_reconstruct import redis_aof_reconstruct,\
  c_aof_archive_collector
from redisrollforward.redis_aof_recreate import c_aof_reconstruct_request_executor
from redisrollforward import redis_aof_recreate


'''aof reconstruct and recreate tst source'''

print 'start aof_reconstruct'

oRedisAofReconstruct = redis_aof_reconstruct('e:/aof_reconstruct.json', bVerboseIP=True)  
oRedisAofReconstruct.Start()

# From client: tag, port, server and datetime to determine the source aof's to reconstruct until a specific point in timne

tReconstructParam =  json.loads("""
{
  "redistag"             : "dwan",
  "redisport"            : "14143", 
  "server"               : "127.0.0.1",
  "redis_server_summary" : "testM.tst.dwan__14803", 
  "atdatetime"           : "2015-04-04_17-00-00"
}
""", object_pairs_hook=dict) 

start = time.time()

print 'init aof collector'
  
oAofCollector = c_aof_archive_collector(oRedisAofReconstruct.tConfig['aofbackupdir'], tReconstructParam)
print 'total collected chunks', oAofCollector.iChunkTotal
    
print 'collect time', time.time() - start

print 'request aofchunks and reconstruct appendonly.aof '  
start = time.time()
oReconstructorExec = c_aof_reconstruct_request_executor('e:/tsttst/appendonly.aof', 'localhost', 49185)
print oReconstructorExec.RequestAndReconstruct(json.dumps(tReconstructParam))

print 'request and reconstruct time', time.time() - start

# redis_aof_reconstruct server
exit() 

# Works only in a linux enviroment  
# oLoader = c_redis_db_aof_restarter(cWorkdirIP=cWorkdir, cRedisTagIP=cRedisTag, iRedisPortIP=iRedisPort, cRedisConfpathIP = cRedisConfpath, cRedisStartupCommandIP = cRedisStartupCommand) 
# oLoader.RestartDbAndLoadAof()

tReconstructParam =  json.loads("""{ "redistag" : "dwan", "redisport"  : "14143", "server" : "127.0.0.1", "redis_server_summary" : "testM.tst.dwan__14803","atdatetime" : "2015-04-04_17-00-00" }""", object_pairs_hook=dict)
oRedisAofRecreate = redis_aof_recreate('')
oRedisAofRecreate.RecreateRedisDb(tReconstructParam)
  
# EOF
  