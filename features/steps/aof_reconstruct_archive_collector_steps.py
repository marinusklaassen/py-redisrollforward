import os
import sys
import json

from redisrollforward.redis_aof_reconstruct import c_aof_archive_collector

from behave import given, when, then, step  # a@UnresolvedImport @UnusedImport

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

cTestdir     = os.path.join(cAmberEclipseWorkspace, 'py-redisrollforward/features/steps')
cTestdataDir = os.path.join(cTestdir,       'testdata')

def RemoveDirWithContents(cDirIP):
  
  for root, dirs, files in os.walk(cDirIP, topdown=False):
      for name in files:
          os.remove(os.path.join(root, name))
      for name in dirs:
          os.rmdir(os.path.join(root, name))
  os.rmdir(cDirIP)

def Teardown(context): 

  if bDeleteTestData:
    RemoveDirWithContents(cTestdataDir)

# Feature steps 

@given(u'a server backup directory "{cAofBackupDir}" for server address "{cServerAddressIP}"')
def CreateServerBackupDir(oContextIP, cAofBackupDir, cServerAddressIP):
    
    oContextIP.cAofBackupDir = os.path.join(cTestdataDir, cAofBackupDir)
    oContextIP.cServerDir    = os.path.join(oContextIP.cAofBackupDir, cServerAddressIP)
    if os.path.isdir(cTestdataDir): 
      RemoveDirWithContents(cTestdataDir)
    os.makedirs(oContextIP.cServerDir)

@given(u'a series of archived aofchunks located in several subfolders')
def CreateArchivedAofchunks(oContextIP): 

  for tRow in oContextIP.table:
    cAofBackupDir = os.path.join(oContextIP.cServerDir, tRow['Directory'])
    os.makedirs(cAofBackupDir)
    open(os.path.join(cAofBackupDir, tRow['Filename']), 'wb').close()

@given(u'an instance of the c_aof_archive_collector')
def InstantiateAofArchiveCollector(oContextIP):
  
  oContextIP.oAofArchiveCollector = c_aof_archive_collector(oContextIP.cAofBackupDir)

@when(u'I request the archived aofchunks with the json instruction')
def RequestAofChunks(oContextIP):
  
  tReconstructParam =  json.loads(oContextIP.text)  # @UnusedVariable
  oContextIP.oAofArchiveCollector.CollectAofchunks(tReconstructParam)

@then('I see {iNumAofchunksIP:n} collected files')
def AssertNumAofchunks(oContextIP, iNumAofchunksIP):
  
  iAofchunks = oContextIP.oAofArchiveCollector.GetTotalChunks()
  RemoveDirWithContents(cTestdataDir)
  assert iAofchunks == iNumAofchunksIP, 'Got %s Expected %s' % (iAofchunks, iNumAofchunksIP)
  
#EOF 
