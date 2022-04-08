import os
import sys
import time
import redis
import random
import subprocess

from redisrollforward.redis_aof_read   import redis_aof_read  
from redisrollforward.redis_aof_funnel import redis_aof_funnel 

from behave import given, when, then, step  # a@UnresolvedImport @UnusedImport
from datetime import datetime

# Get environment variables
try:
  bDeleteTestData = os.getenv('DeleteTestData') == 'True'
except:
  bDeleteTestData = True

# Globals 
cAmberEclipseWorkspace = os.getenv('AmberEclipseWorkspace')

if sys.platform == 'win32':
  cRedisStartupCommand = os.path.join(cAmberEclipseWorkspace, '.metadata/.plugins/org.eclipse.debug.core/.launches/tools/Redis-Msdn-2.6_alpha_x64/redis-server.exe')
else:
  cRedisStartupCommand = 'redis-server' 

cTestdir         = os.path.join(cAmberEclipseWorkspace, 'py-redisrollforward/features/steps')
cTestConfigdir   = os.path.join(cTestdir,       'config')
cTestdataDir     = os.path.join(cTestdir,       'testdata')
cRedisSrcConfig  = os.path.join(cTestConfigdir, 'redis.conf')
cRedisDstConfig  = os.path.join(cTestdataDir,   'redis.conf')
cAofBackupDir    = os.path.join(cTestdataDir,   '127.0.0.1', datetime.now().strftime('%Y-%m-%d'))


# Helper functions 
def ScrambleString(cIP):
  return ''.join(random.sample(cIP, len(cIP))) 

def RemoveDirWithContents(cDirIP):
  
  for root, dirs, files in os.walk(cDirIP, topdown=False):
      for name in files:
          os.remove(os.path.join(root, name))
      for name in dirs:
          os.rmdir(os.path.join(root, name))
  os.rmdir(cDirIP)

def ConfigCopyAndUpdate(cRedisSrcConfigIP, cRedisDstConfigIP, cTestdataDirIP): 
  '''Config the original config of a redis instance to the workdir and update the appendonly settings'''
  
  with open(cRedisSrcConfigIP, 'rb') as oRead, open(cRedisDstConfigIP, 'wb') as oWrite:
    for cLine in oRead:
      if len(cLine.rstrip()) > 0 and not cLine.startswith('#'):
        tLine = cLine.split()
        cPropertyName  = tLine[0]
        if cPropertyName == 'logfile':
          cLine = 'logfile %s\n' % os.path.join(cTestdataDirIP, 'redis_6379.log')  
        elif cPropertyName == 'pidfile':
          cLine = 'pidfile %s\n' % os.path.join(cTestdataDirIP, 'redis_6379.pid')
        elif cPropertyName == 'dir':
          cLine = 'dir %s\n' % cTestdataDirIP
      oWrite.write(cLine)

def Teardown(context): 

  context.oRedisClient.shutdown()
  open(os.path.join(cTestdataDir, 'clientdisconnect_%s.trg' % os.getpid()), 'wb').close()
  context.oRedisAofFunnel.ServerStop()
  time.sleep(1.0)
  context.oRedisAofRead.ClientStop()
  context.oRedisAofRead.oLog.handlers[0].close()
  context.oRedisAofFunnel.oLog.handlers[0].close()
  # Stopping aof_read and aof_funnel takes some time, so wait
  time.sleep(1.0)
  if bDeleteTestData:
    RemoveDirWithContents(cTestdataDir)

# Feature steps 

@given(u'there is a redis instance with AOF persistence that logs every write operation received by the server')
def StartRedisDb(context):
    
    # Shutdown the default redis db if running
    context.oRedisClient = redis.StrictRedis('localhost', 45321)
    context.oRedisClient.shutdown()
    time.sleep(1.0) 
    # First reset testdata dir
    if os.path.isdir(cTestdataDir): 
      RemoveDirWithContents(cTestdataDir)
    os.makedirs(cTestdataDir)
    # Redis Db copy and prepare
    ConfigCopyAndUpdate(cRedisSrcConfig, cRedisDstConfig, cTestdataDir)
    subprocess.Popen([cRedisStartupCommand, cRedisDstConfig])
    # Wait a short moment for the redis instance to boot
    time.sleep(0.5) 
  
@given(u'there is a running redis_aof_read client')
def StartRedisAofRead(context):
 
    context.oRedisAofRead = redis_aof_read(os.path.join(cTestConfigdir, 'test_aof_read.json'), bVerboseIP = True)
    context.oRedisAofRead.ClientStart()

@given(u'there is a running redis_aof_funnel server')
def StartRedisAofFunnel(context):

  context.oRedisAofFunnel = redis_aof_funnel(os.path.join(cTestConfigdir, 'test_aof_funnel.json'), bVerboseIP = True)
  context.oRedisAofFunnel.ServerStart()

@when(u'I make a change in the Redis database')
def ApplyChangeInRedisDb(context): 

  tKey = ('aapjes', 'nootjes', 'miesjes', 'schaapjes')
  context.oRedisClient.set(tKey[random.randrange(len(tKey))], ScrambleString(tKey[random.randrange(len(tKey))]))

@when(u'repeat \'I make a change in the Redis database\' {iAmountIP:n} times while waiting {iSecondIP:n} second each turn')
def RepeatApplyChangeInRedisDb(context, iAmountIP, iSecondIP):
  
  iRepetition = 0
  while iRepetition <= iAmountIP:
    context.execute_steps(u'When I make a change in the Redis database')
    iRepetition += 1
    time.sleep(iSecondIP)
  time.sleep(1.0)
  
    
@then(u'I see {iFileCountIP:n} archived files in the backup folder')
def AssertArchivedFileCount(context, iFileCountIP):
  
  tFilename = [cFile for _root, _dir, tFiles in os.walk(cAofBackupDir) for cFile in tFiles]
  iNumFiles = len(tFilename)
  Teardown(context)
  # Count the aofchunk files in the testdata aofbackup folder
  assert iNumFiles == iFileCountIP, 'Got %s Expected %s' % (iNumFiles, iFileCountIP)
  
#EOF
