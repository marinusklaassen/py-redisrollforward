import os
import re
import sys
import json
import signal
import datetime
import traceback

from amber.msroot.msutil.logic.sc_msstring_expandosenv import sc_msstring_expandosenv

from redisrollforward.common.redis_aof_common import sc_log
from redisrollforward.common.redis_aof_common import GenDateRange
from redisrollforward.common.redis_aof_common import aof_server_common
from redisrollforward.common.redis_aof_common import c_base_request_handler
from redisrollforward.common.redis_aof_common import c_socket_packedmessage

xExpandString=sc_msstring_expandosenv.ExpandString

'''See docstring of redis_aof_reconstruct class'''

class c_aof_archive_collector(object): 
  '''Aofchunk collector 
     First: Call the CollectAofchunks method before using the iter functionality of this class
     Then: Call the instance as a iterable with 'next'. The content of the sorted aofchunks will be returned 
  '''
  
  def __init__(self, cAofBackupdirIP): 
  
    self.ResetState()  
    self.cAofBackupdir = cAofBackupdirIP
  
  def __iter__(self):
    
    return self
  
  def next(self): 
    '''Get the next file content. First class CollectAofchunks before using the iter functionality of this class'''
    
    if self.tAofChunkpath: 
      self.iChunkNum += 1
      cFilename, cFilepath = self.tAofChunkpath.pop(0)
      return (self.iChunkNum, self.iChunkTotal, self.cFileExt, cFilename, open(cFilepath, "rb").read())
    else: 
      raise StopIteration
 
  def UnpackReconstructParam(self, tReconstructParamIP):
    
    cNamePrefix      ='{0[redistag]}_{0[redisport]}'.format(tReconstructParamIP)
    cMaxDatetime     = tReconstructParamIP['atdatetime']
    cServer          = tReconstructParamIP['server']
    return cNamePrefix, cMaxDatetime, cServer
 
  def ResetState(self): 
  
    self.tAofChunkpath = []
    self.iChunkTotal   = 0
    self.iChunkNum     = 0
  
  def GetTotalChunks(self):
    
    return len(self.tAofChunkpath)
 
  def HasCollectedAofchunks(self): 
    
    return self.GetTotalChunks() > 0
  
  def CollectAofchunks(self, tReconstructParamIP):
    """Collect aof chunks from archive instructed by a dictionary with the following key values:  
       {
         "redistag"   : "dwan",
         "redisport"  : "6379", 
         "server"     : "127.0.0.1",
         "atdatetime" : "2015-04-01_04-44-44"
       }
    """
    
    self.ResetState()
    cNamePrefix, cMaxDatetime, cServer = self.UnpackReconstructParam(tReconstructParamIP)
    cDate                              = cMaxDatetime.split('_')[0]
    self.cAofSearchDir                 = os.path.join(self.cAofBackupdir, cServer, cDate) 
    cLatestAofChunkname                = self.SearchForLatestAofChunkname(self.cAofSearchDir, cNamePrefix, cMaxDatetime)
    # At least 1 aofchunk is 
    if len(cLatestAofChunkname) > 0:
      cLatestAofname, cFileExt = os.path.splitext(cLatestAofChunkname)
      self.cFileExt            =  cFileExt[1:]
      # Because archived file portion at with a specific datetime stamp can consist of multiple aofchunks
      cLatestAofname           = '{}_999999'.format(cLatestAofname)
      cBgRewriteAofname        = self.GetFirstAofname(cLatestAofname)
      self.CollectAofChunkpathsFromAofBackupDir(cServer, cBgRewriteAofname, cLatestAofname)
      self.iChunkTotal         = self.GetTotalChunks()
      self.tAofChunkpath.sort()
  
  def SearchForLatestAofChunkname(self, cAofSearchDirIP, cNamePrefixIP, cMaxDatetimeIP):
    '''Search in the server date subfolder for the latest archived aofchunk before the requested max datetime'''
    
    cLatestAofname   = '' 
    cLatestTimestamp = '' 
    for dummy, dummy, tFile in os.walk(cAofSearchDirIP):
      for cFilename in tFile:
        if cFilename.startswith(cNamePrefixIP):
          cTimestamp = cFilename.split('_-_')[1].split('__')[0]
          if cTimestamp > cLatestTimestamp and cTimestamp <= cMaxDatetimeIP:
            cLatestAofname   = cFilename 
            cLatestTimestamp = cTimestamp
    return cLatestAofname
 
  def GetFirstAofname(self, cFilenameIP): 
    '''input   dwan_14143__2015-04-09_10-50-06_-_2015-04-09_12-50-27
       returns dwan_14143__2015-04-09_10-50-06_-_2015-04-09_10-50-06
    '''
    tFilename = cFilenameIP.partition('_-_')
    return tFilename[0] + tFilename[1] + tFilename[0].split('__')[1] 
  
  def GetDateTimestamp(self, cAofnameIP): 
    
    cDate  = cAofnameIP.split('_-_')[1].split('_')[0]
    return datetime.datetime.strptime(cDate, '%Y-%m-%d').date()
  
  def CollectAofChunkpathsFromAofBackupDir(self, cServerIP, cBgRewriteAofnameIP, cLatestAofnameIP):
    '''Search and Collect aofchunks in the date folders by a range between bgrewrite date en timestamp date'''
    
    self.tAofChunkpath = [] 
    for dtIter in GenDateRange(self.GetDateTimestamp(cBgRewriteAofnameIP), self.GetDateTimestamp(cLatestAofnameIP)):
      self.CollectAofChunkpathsFromDtFolder(os.path.join(self.cAofBackupdir, cServerIP, str(dtIter)), cBgRewriteAofnameIP, cLatestAofnameIP)
  
  def CollectAofChunkpathsFromDtFolder(self, cDirIP, cBgRewriteAofnameIP, cLatestAofnameIP):
    '''Search and Collect aofchunks from a directory'''
    
    for cRoot, dummy, tFile in os.walk(cDirIP):
      for cFilename in tFile:
        if cFilename >= cBgRewriteAofnameIP and cFilename <= cLatestAofnameIP: 
          self.tAofChunkpath.append((cFilename, os.path.join(cRoot, cFilename)))

class c_aof_reconstruct_server_request_handler(c_base_request_handler, c_socket_packedmessage):
  '''The request handler for the aof_read_reconstruct server. This class 
     will be instantiated once per established connection with a client 
  '''
  
  def ServerRequestHandler(self): 
     
    cReveivedMessage  = None
    cResponseToClient = None
    try:
      cReveivedMessage = self.PackedMessageRcv()
      # partition request in descriptor, division char and payload
      tReceivedMessage = cReveivedMessage.partition('\n\x80\n')
      # handle request 
      if cReveivedMessage == 'stop_redis_aof_reconstruct':
          cResponseToClient = 'Closing Aof Reconstruct Server'
          self.oServer.ServerStop()
      elif tReceivedMessage[1] == '\n\x80\n': 
        tMessageProperties = json.loads(tReceivedMessage[0], object_pairs_hook=dict)
        cMessageData       = tReceivedMessage[2]
        if tMessageProperties['cType'] == 'Request' and tMessageProperties['tDescriptor']['cDataType'] == 'json':
          tReconstructParam                           = json.loads(cMessageData, object_pairs_hook=dict)
          bIsValid, cResponseToClient = self.oServer.oMain.IsValidReconstructParam(tReconstructParam)
          if bIsValid:
            # Initialize aof chunk collector generator for the source server folder in the aofbackupdir
            oAofCollector = c_aof_archive_collector(self.oServer.oMain.tConfig['aofbackupdir'])
            oAofCollector.CollectAofchunks(tReconstructParam) 
            # Aof Collector has found aofchunks now send them to the client
            if oAofCollector.HasCollectedAofchunks():
              # Send each aofchunk to redis_aof_recreate
              for iChunkNum, iChunkTotal, cDataType, cFilename, cPayload in oAofCollector:
                tDescriptor = {}
                tDescriptor['cType']                      = 'Payload'
                tDescriptor['tProperties']                = {}
                tDescriptor['tProperties']['iLength']     = len(cPayload)
                tDescriptor['tProperties']['iChunkNum']   = iChunkNum
                tDescriptor['tProperties']['iChunkTotal'] = iChunkTotal
                tDescriptor['tProperties']['cDataType']   = cDataType
                tDescriptor['tProperties']['cFilename']   = cFilename
                # Send aofchunk data to the recreate client 
                self.PackedMessageSend('%s\n\x80\n%s' % (json.dumps(tDescriptor), cPayload))
                cResponseToClient = self.PackedMessageRcv()
                if not cResponseToClient == 'part_okay':
                  cResponseToClient = 'error_while_sending_payloads'
                  break 
              if cResponseToClient == 'part_okay': cResponseToClient = 'Finished reconstruct request: {} chunks are transferred by aof reconstruct'.format(iChunkTotal)
            else: 
              cResponseToClient = 'invalid_request: No aof chunks found in directory {} with the request parameters {}'.format(oAofCollector.cAofSearchDir, tReconstructParam)
    except Exception:
      cResponseToClient = 'invalid_request: %s\n' % traceback.format_exc()
      self.oLog.error(cResponseToClient)
    finally: 
      # Always try to send a response back
      if cResponseToClient is None:
        cResponseToClient = 'Unexpected error: No response data to return'
      try:
        self.PackedMessageSend('{}\n\x80\n{}'.format('{ "cType": "Response" }', cResponseToClient))
      except Exception:
        self.oLog.error(traceback.format_exc())
  
class redis_aof_reconstruct(aof_server_common):
  
  '''
  AOF Reconstruct Server: redis_aof_reconstruct
  
  TCP/IP SocketServer:
  
  Process requests asynchronously by creating a separate process or thread to handle each client request. 
  
  Automatically collects archived aofchunks  
  
  To stop the reconstruct process send 'stop_redis_aof_reconstruct' via a socket connection. 
  The reconstruct process will stop accepting incoming server requests. When each request 
  thread is finished this process will then quit. -OR kill this process with kill -15
  '''
  
  def __init__(self, cConfigpathIP, bVerboseIP = False): 
    
    aof_server_common.__init__(self)
    self.ConfigLoad(cConfigpathIP)
    self.ConfigAdapt()
    self.ValidateConfig()
    self.bVerbose              = bVerboseIP or self.HasForceVerboseFileInLogdir()
    self.xServerRequestHandler = c_aof_reconstruct_server_request_handler
    self.cServername           = 'Aof Reconstruct'  
    self.tServerAddress        = (self.tConfig['reconstructhost'], self.tConfig['reconstructport'])
    self.oLog                  = sc_log.GetLogger(self.GetLogpath())
  
  def ConfigAdapt(self):
    '''Perform type conversion of specific config entries'''
  
    self.tConfig['logdir']          = xExpandString(self.tConfig['logdir'])
    self.tConfig['aofbackupdir']    = xExpandString(self.tConfig['aofbackupdir'])
    self.tConfig['reconstructhost'] = str(self.tConfig['reconstructhost'])
    self.tConfig['reconstructport'] = int(self.tConfig['reconstructport'])
  
  def ValidateConfig(self): 
    
    if not os.path.isdir(self.tConfig['aofbackupdir']):
      raise Exception('backup directory %s does not exist' % self.tConfig['aofbackupdir'])
  
  def IsValidReconstructParam(self, tReconstructParamIP):
    
    cMessage = '' 
    tKeys    = tReconstructParamIP.keys()
    bIsValid = type(tReconstructParamIP) is dict and 'atdatetime' in tKeys and 'redistag' in tKeys and 'redisport' in tKeys
    if bIsValid: 
      cDatetime = tReconstructParamIP['atdatetime'].rstrip()
      if re.match('^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',  cDatetime): 
        tReconstructParamIP['atdatetime'] = cDatetime.replace(':', '-').replace(' ', '_')
      elif not re.match('^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$', cDatetime): 
        cMessage = 'Invalid date & time: {}\nThe correct formats are yyyy-MM-dd hh:mm:ss or yyyy-MM-dd_HH_mm_s'.format(cDatetime)
    else: 
        cMessage = 'Invalid reconstruct request:\nA valid json request must contain the keys atdatetime, redistag and redisport'
    return bIsValid, cMessage
  
  def GetLogpath(self):
    
    cLogname = 'redis_aof_reconstruct_{0[reconstructhost]}_{0[reconstructport]}_{1}.log'.format(self.tConfig, datetime.date.today().isoformat()) 
    cLogpath = os.path.join(self.tConfig['logdir'], cLogname)
    return cLogpath
  
  def ServerStart(self): 
    
    self.oLog.info('Start {} with host={}: port={}'.format(self.cServername, self.tConfig['reconstructhost'], self.tConfig['reconstructport']))
    aof_server_common.ServerStart(self)
    
if __name__ == '__main__':
  '''
  Always startup with 1 parameter: the path to the config file.
  '''
  
  if len(sys.argv) < 2:
    print 'Missing first argument: the filepath to the aof_reconstruct.json config of a specific redis instance. Look in the "examples" dir for documentation.'
    sys.exit(1)
  
  if not os.path.isfile(sys.argv[1]):
    print 'The config file {} does not exist'.format(sys.argv[1])
    sys.exit(2)
 
  if len(sys.argv) == 3:
    bVerbose = sys.argv[2] == True 
  else: 
    bVerbose = False
   
  cConfigpath = sys.argv[1]  
  
  oRedisAofReconstruct = redis_aof_reconstruct(cConfigpath, bVerboseIP=bVerbose)  
  oRedisAofReconstruct.ServerStart()
    # Register the redis_aof_roundtripcheck instance method handler to POSIX unix signal events.  
  signal.signal(signal.SIGINT,  oRedisAofReconstruct.ServerStop)
  signal.signal(signal.SIGTERM, oRedisAofReconstruct.ServerStop)
  # Wait for shutdown request
  oRedisAofReconstruct.WaitForServerStop()
  
  exit() # At this point stop all processing
  
# EOF
