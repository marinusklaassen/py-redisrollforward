import os
import sys
import json

from redisrollforward.redis_aof_recreate    import redis_aof_recreate
from redisrollforward.redis_aof_reconstruct import redis_aof_reconstruct

from behave import given, when, then, step  # a@UnresolvedImport @UnusedImport
from redisrollforward.common.redis_aof_common import c_socketclient
import redis
import time

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

cTestdir       = os.path.join(cAmberEclipseWorkspace, 'py-redisrollforward/features/steps')
cTestdataDir   = os.path.join(cTestdir,               'testdata')
cTestConfigdir = os.path.join(cTestdir,               'config')
cRedisSrcConfig  = os.path.join(cTestConfigdir,       'redis.conf')
cRedisDstConfig  = os.path.join(cTestdataDir,         'redis.conf')

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

def RemoveDirWithContents(cDirIP):
  
  for root, dirs, files in os.walk(cDirIP, topdown=False):
      for name in files:
          os.remove(os.path.join(root, name))
      for name in dirs:
          os.rmdir(os.path.join(root, name))
  os.rmdir(cDirIP)

def Teardown(context): 

  context.oRedisAofRecreate.ServerStop()
  context.oRedisAofRecreate.ServerStop()
  if bDeleteTestData:
    RemoveDirWithContents(cTestdataDir)
    
# Feature steps 

@given(u'a backup directory "{cAofBackupDir}" for server address "{cServerAddressIP}"')
def CreateServerBackupDir(oContextIP, cAofBackupDir, cServerAddressIP):
  
  oContextIP.cAofBackupDir = os.path.join(cTestdataDir, cAofBackupDir)
  oContextIP.cServerDir    = os.path.join(oContextIP.cAofBackupDir, cServerAddressIP)
  if os.path.isdir(cTestdataDir): 
    RemoveDirWithContents(cTestdataDir)
  os.makedirs(oContextIP.cServerDir)
  ConfigCopyAndUpdate(cRedisSrcConfig, cRedisDstConfig, cTestdataDir)

@given(u'a series of archived aofchunks located in several subfolders 1')
def CreateArchivedAofchunks(oContextIP): 
  
  oContextIP.tAofChunkpath = {}

  for tRow in oContextIP.table:
    cAofBackupDir = os.path.join(oContextIP.cServerDir, tRow['Directory'])
    os.makedirs(cAofBackupDir)
    oContextIP.tAofChunkpath[tRow['Filename']] = os.path.join(cAofBackupDir, tRow['Filename'])
    open(oContextIP.tAofChunkpath[tRow['Filename']], 'wb').close()

@given(u'dwan_6379__2015-03-30_02-00-00_-_2015-03-30_02-00-00__000001_000001.aof contains')
def FillAofChunk1(oContextIP):
  
  cFilepath = oContextIP.tAofChunkpath['dwan_6379__2015-03-30_02-00-00_-_2015-03-30_02-00-00__000001_000001.aof']
  with open(cFilepath, 'wb') as oFile:
    oFile.write(oContextIP.text) 

@given(u'dwan_6379__2015-03-30_02-00-00_-_2015-03-31_02-00-00__000001_000001.aof contains')
def FillAofChunk2(oContextIP):
  
  cFilepath = oContextIP.tAofChunkpath['dwan_6379__2015-03-30_02-00-00_-_2015-03-31_02-00-00__000001_000001.aof']
  with open(cFilepath, 'wb') as oFile:
    oFile.write(oContextIP.text) 

@given(u'dwan_6379__2015-03-30_02-00-00_-_2015-04-01_02-00-00__000001_000001.aof contains')
def FillAofChunk3(oContextIP):
  
  cFilepath = oContextIP.tAofChunkpath['dwan_6379__2015-03-30_02-00-00_-_2015-04-01_02-00-00__000001_000001.aof']
  with open(cFilepath, 'wb') as oFile:
    oFile.write(oContextIP.text) 

@given(u'there is a running redis_aof_reconstruct server')
def StartRedisAofReconstruct(context):
 
    context.oRedisAofReconstruct = redis_aof_reconstruct(os.path.join(cTestConfigdir, 'test_aof_reconstruct.json'), bVerboseIP = True)
    context.oRedisAofReconstruct.ServerStart()

@given(u'there is a running redis_aof_recreate server')
def StartRedisAofRecreate(context):

  context.oRedisAofRecreate = redis_aof_recreate(os.path.join(cTestConfigdir, 'test_aof_recreate.json'), bVerboseIP = True)
  context.oRedisAofRecreate.ServerStart()

@when(u'I request a redis database state with following the json instruction via a socket')
def RequestDbState(oContextIP):
  
  oSocketClient = c_socketclient('localhost', 49997)
  oSocketClient.ClientConnectToServer()
  tRecreateparm =  json.loads(oContextIP.text)
  tReconstructparm = oContextIP.oRedisAofRecreate.AddParmsForReconstruct(tRecreateparm)
  oSocketClient.PackedMessageSend(str(json.dumps(tReconstructparm)))
  cReponseMsg = oSocketClient.PackedMessageRcv()  # @UnusedVariable
  oSocketClient.ClientSocketClose()
  
@then('I see a redis instance on localhost and port 45321 with the following key values')
def AssertNumAofchunks(oContextIP):
  
  oRedisClient = redis.StrictRedis('localhost', 45321)
  for tRow in oContextIP.table:
    cKey     = tRow['key']
    cValue   = tRow['value'].rstrip()
    cDbValue = oRedisClient.get(cKey.rstrip())  
    assert cDbValue == cValue, 'Got %s Expected %s' % (cValue, cDbValue)
  oRedisClient.shutdown()  
  oContextIP.oRedisAofReconstruct.ServerStop()
  oContextIP.oRedisAofRecreate.ServerStop()
  time.sleep(1.0)
  oContextIP.oRedisAofReconstruct.oLog.handlers[0].close()
  oContextIP.oRedisAofRecreate.oLog.handlers[0].close()
  RemoveDirWithContents(cTestdataDir)
  
#EOF 
