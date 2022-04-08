import os
import json
import signal
import socket
import datetime
import traceback
import sys

from amber.msroot.msutil.logic.sc_msstring_expandosenv import sc_msstring_expandosenv
from redisrollforward.common.redis_aof_common          import c_base_request_handler
from redisrollforward.common.redis_aof_common          import c_socket_packedmessage
from redisrollforward.common.redis_aof_common          import aof_server_common
from redisrollforward.common.redis_aof_common          import sc_log

xExpandString=sc_msstring_expandosenv.ExpandString

'''See docstring of redis_aof_funnel class'''

class c_aof_funnel_server_request_handler(c_base_request_handler, c_socket_packedmessage):
  '''The request handler for the aof_read_funnel server. This class 
     will be instantiated once per established connection with a client 
  '''
  
  def ServerRequestHandler(self): # FREF@950ac5b82: c_aof_funnel_tcprequest_handler
    
    while not self.bShutdownRequested:
      cResponseToClient = '' 
      try:
        cReveivedMessage = self.PackedMessageRcv()
        if cReveivedMessage == 'close_connection': 
          self.PackedMessageSend('Aof Funnel is closing this connection')
          self.RequestHandlerStop()
        elif cReveivedMessage == 'stop_redis_aof_funnel':
          cResponseToClient = 'Closing Aof Funnel Server'
          self.oServer.ServerStop()
        elif not cReveivedMessage is None and len(cReveivedMessage) > 0: 
          cResponseToClient = self.oServer.oMain.oAofchunkArchiver.PersistAofchunk(cReveivedMessage, self.tClientAddress)
      except socket.error:
        self.oLog.error(traceback.format_exc())
        self.oLog.info('A client with host=%s and port=%s is disconnected' % self.tClientAddress)
        self.RequestHandlerStop()
      except Exception:
        if cResponseToClient != 'part_okay': 
          cResponseToClient = 'Error while processing request: %s' % traceback.format_exc()
          self.oLog.error(cResponseToClient)
      finally: 
        # Always try to send a response back
        if len(cResponseToClient) == 0:
          cResponseToClient = 'Unexpected error: No response data to return'
        try:
          self.PackedMessageSend(cResponseToClient)
        except socket.error: 
          self.oLog.error(traceback.format_exc())
          self.oLog.error('A client with host=%s and port=%s is disconnected' % self.tClientAddress)
          self.RequestHandlerStop()

class c_aofchunk_archiver_filesystem(object): 
  
  def __init__(self, cAofBackupDirIP):
  
    self.cAofBackupdir = cAofBackupDirIP
  
  def PersistAofchunk(self, cReveivedMessageIP, tClientAddressIP):
    
    cResult = '' 
    try:
      cAofname, iAofBytesize, bDataCompressed, cPayload  = self.UnpackMessage(cReveivedMessageIP)
      if iAofBytesize == len(cPayload): 
        cAofBackupdir = self.PrepareBackupdir(tClientAddressIP, cAofname)
        # Received aof chunk will be archived like: '/var/aofbackup/192.168.100.10/2015-03-13/dwan_6379__2015-03-13_23-46-59_-_2015-03-13_23-51-44.lz4'  
        cAofChunkpath = os.path.join(cAofBackupdir, '%s%s' % (cAofname, self.GetFileExt(bDataCompressed))) 
        if self.PersistAofchunkAsFile(cAofChunkpath, cPayload):
          cResult = 'part_okay'
    except Exception:
      cResult = traceback.format_exc()
    return cResult

  def PersistAofchunkAsFile(self, cAofChunkpathIP, cPayloadIP): 
    
    with open(cAofChunkpathIP, 'wb') as oFile:
      oFile.write(cPayloadIP)
    # Succesfully written to archive
    return os.path.isfile(cAofChunkpathIP) and (os.stat(cAofChunkpathIP).st_size > 0)

  def GetFileExt(self, bDataCompressedIP):
    
    if bDataCompressedIP:
      return '.lz4'
    else: 
      return '.aof'
  
  def PrepareBackupdir(self, tClientAddressIP, cAofnameIP):
    
    cBackupdir = os.path.join(self.cAofBackupdir, tClientAddressIP[0], self.GetBgRewriteAofDateAsString(cAofnameIP)) 
    if not os.path.exists(cBackupdir):   
      os.makedirs(cBackupdir)
    return cBackupdir

  def UnpackMessage(self, cReveivedMessageIP):

    # Split the received data into a descriptor and payload
    tData           = cReveivedMessageIP.partition('\n\x80\n')
    tDescription    = json.loads(tData[0], object_pairs_hook=dict)  
    cAofname        = tDescription['aof_read_backup_filename'] 
    iAofBytesize    = tDescription['aof_read_backup_filebytesize']
    bDataCompressed = tDescription['data_compressed'] 
    cPayload        = tData[2] 
    return cAofname, iAofBytesize, bDataCompressed, cPayload

  def GetBgRewriteAofDateAsString(self, cAofnameIP):
    return cAofnameIP.split('_-_')[1].split('_')[0]
  
class redis_aof_funnel(aof_server_common): 
  '''
  AOF Backup Server: redis_aof_funnel
  
  TCP/IP SocketServer:
  
  Process requests asynchronously by creating a separate process or thread to handle each client request. 
  
  Automatically archives incoming aof_read sections 
  
  To stop the funnel process send 'stop_redis_aof_funnel' via a socket connection
  
  Before stopping the redis_aof_funnel each client connected has to send a stop_connection message, -OR 
  - Kill its process with kill -15
  '''
  
  def __init__(self, cConfigpathIP, bVerboseIP = False): 
    
    aof_server_common.__init__(self)
    self.ConfigLoad(cConfigpathIP)
    self.ConfigAdapt()
    self.bVerbose              = bVerboseIP or self.HasForceVerboseFileInLogdir()
    self.xServerRequestHandler = c_aof_funnel_server_request_handler
    self.cServername           = 'Aof Funnel'  
    self.tServerAddress        = (self.tConfig['funnelhost'], self.tConfig['funnelport'])
    self.oAofchunkArchiver     = c_aofchunk_archiver_filesystem(self.tConfig['aofbackupdir']) 
    self.oLog                  = sc_log.GetLogger(self.GetLogpath())
    
  def ConfigAdapt(self):
    '''Perform type conversion of specific config entries'''
  
    self.tConfig['logdir']       = xExpandString(self.tConfig['logdir'])
    self.tConfig['aofbackupdir'] = xExpandString(self.tConfig['aofbackupdir'])
    self.tConfig['funnelhost']   = str(self.tConfig['funnelhost'])
    self.tConfig['funnelport']   = int(self.tConfig['funnelport'])
    
  def GetBackupdir(self):
    return self.tConfig['aofbackupdir']
  
  def GetLogpath(self):
    
    cLogname = 'redis_aof_funnel_{0[funnelhost]}_{0[funnelport]}_{1}.log'.format(self.tConfig, datetime.date.today().isoformat()) 
    cLogpath = os.path.join(self.tConfig['logdir'], cLogname)
    return cLogpath
  
  def ServerStart(self): 

    self.oLog.info('Start {} with host={} port={}'.format(self.cServername, self.tConfig['funnelhost'], self.tConfig['funnelport']))
    aof_server_common.ServerStart(self)
       
if __name__ == "__main__":
  '''
  Always startup with 1 parameter: the path to the config file.
  '''
  
  if len(sys.argv) < 2:
    print 'Missing first argument: the filepath to the aof_funnel.json config of a specific redis instance. Look in the "examples" dir for documentation.'
    sys.exit(1)
  
  if not os.path.isfile(sys.argv[1]):
    print 'The config file {} does not exist'.format(sys.argv[1])
    sys.exit(2)
  
  if len(sys.argv) == 3:
    bVerbose = sys.argv[2] == True 
  else: 
    bVerbose = False
  
  cConfigpath = sys.argv[1]
  
  oRedisAofFunnel = redis_aof_funnel(cConfigpath, bVerboseIP=bVerbose) 
  # Register the redis_aof_roundtripcheck instance method handler to POSIX unix signal events.  
  signal.signal(signal.SIGINT,  oRedisAofFunnel.ServerStop)
  signal.signal(signal.SIGTERM, oRedisAofFunnel.ServerStop)
  oRedisAofFunnel.ServerStart()
  # Wait for shutdown request
  oRedisAofFunnel.WaitForServerStop()
  
  exit() # At this point stop all processing
  
#EOF
