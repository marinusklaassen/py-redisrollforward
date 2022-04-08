'''See docstring of c_aof_reconstruct_request_executor class'''

import os
import sys
import json
import time
import redis
import socket
import signal
import datetime
import traceback
import threading
import subprocess
import platform

from lz4 import decompress
from collections import OrderedDict
from redis.exceptions import ConnectionError

from amber.msroot.msutil.logic.sc_msstring_expandosenv import sc_msstring_expandosenv

from redisrollforward.common.redis_aof_common import aof_server_common
from redisrollforward.common.redis_aof_common import sc_osfile
from redisrollforward.common.redis_aof_common import c_socketclient, sc_log
from redisrollforward.common.redis_aof_common import c_base_request_handler
from redisrollforward.common.redis_aof_common import c_socket_packedmessage

if platform.system() != 'Windows':
  EXT_EXE = ""
else:
  EXT_EXE = ".exe"

aExpandString=sc_msstring_expandosenv.ExpandString

def _AddParmsForReconstruct(tRecreateRequest):
   
  ''' Translate incoming 'aof_recreate' parameters to 'aof_reconstruct' format '''
   
  if 'source_redisserver' in tRecreateRequest:
    tRecreateRequest['server'] = tRecreateRequest['source_redisserver']
  if 'source_redistag' in tRecreateRequest:
    tRecreateRequest['redistag'] = tRecreateRequest['source_redistag']
  if 'source_redisport' in tRecreateRequest:
    tRecreateRequest['redisport'] = tRecreateRequest['source_redisport']
  return tRecreateRequest

class c_aof_reconstruct(object): 
  
  '''Creates dirs and empties temporary existing appendonly.aof'''
  
  def __init__(self, cFilepathIP):
  
    self.cFilepath = cFilepathIP
    cDir           = os.path.dirname(self.cFilepath)
    if not os.path.exists(cDir):  
      os.makedirs(cDir) 
    self.oFile = open(self.cFilepath, 'wb')
  
  def __enter__(self):
    return self  
  
  def __exit__(self, dummy, value, traceback):
    self.oFile.close() 
    
  def Write(self, cDataIP, bIsCompressedIP = False): 
  
    try: 
      cData = cDataIP 
      if bIsCompressedIP:
        cData = decompress(cData)
      self.oFile.write(cData)
      self.oFile.flush()
      bOK = True
    except: 
      bOK = False
    finally: 
      return bOK

class c_aof_reconstruct_request_executor(c_socketclient):
   
  ''' Request the AOF chunk files from the redis_aof_reconstruct instance 
      and reconstruct the appendonly.aof file by the collected and received AOF chunks
  '''

  def  __init__(self, cFilepathIP, cHostIP, iPortIP): 
    
    self.cFilepath         = cFilepathIP
    c_socketclient.__init__(self, cHostIP, iPortIP)
    
  def RequestAndReconstruct(self, cReconstructParamIP):  
    
    # Request archived AOF chunks from redis_aof_reconstruct
    cRetMessage                             = '' 
    tDescriptor                             = {}
    tDescriptor['cType']                    = 'Request'
    tDescriptor['tDescriptor']              = {} 
    tDescriptor['tDescriptor']['cDataType'] = 'json'  
    try: 
      self.ClientConnectToServer()
      self.PackedMessageSend('%s\n\x80\n%s' % (json.dumps(tDescriptor), cReconstructParamIP))
      with c_aof_reconstruct(self.cFilepath) as oAofReconstructor:
        while True:  
          cMessage                  = self.PackedMessageRcv()
          cDescriptor, dummy, cData = cMessage.partition('\n\x80\n')
          tDescriptor               = json.loads(cDescriptor, object_pairs_hook=dict)  
          if tDescriptor['cType'] == 'Payload': 
            bIsCompressed = tDescriptor['tProperties']['cDataType'] == 'lz4'
            if len(cData) == tDescriptor['tProperties']['iLength'] and oAofReconstructor.Write(cData, bIsCompressed): 
              self.PackedMessageSend('part_okay')  
            else: 
              self.PackedMessageSend('part_failed')
              break 
          elif tDescriptor['cType'] == 'Response':
            cRetMessage = cData 
            break
      self.ClientSocketClose()
    except socket.error:
      cRetMessage = 'Error while making a request to redis_aof_reconstruct.\nIs the redis_aof_reconstruct running and properly binded to its host and port?\n{}'.format(traceback.format_exc())
    except:
      cRetMessage = traceback.format_exc() 
    finally: 
      bOK = cRetMessage.startswith('Finished reconstruct request')
      if bOK: return True, 'Finished recreate request. {}. Redis instance is restarted and recreated with the reconstructed AOF file.'.format(cRetMessage.rstrip('. '))
      else: return False, 'Error: Aborted recreate request. {}.'.format(cRetMessage.rstrip('. '))

class c_aof_redis_db_restarter(object): 
  ''' Restart a redis instance with a loaded appendonly.aof file automatically '''
  
  def __init__(self, cWorkdirIP, cRedisTagIP, iRedisPortIP, cRedisConfpathIP, cRedisStartupCommandIP, oLogIP, tReconstructParamIP):
    
    self.oLog                            = oLogIP
    self.cStartupCommand                 = cRedisStartupCommandIP
    self.cWorkdir                        = cWorkdirIP 
    self.cConfigpath                     = cRedisConfpathIP
    self.cWorkdirConfigpath              = self.ComposeTempConfigpath(self.cConfigpath, cRedisTagIP, iRedisPortIP)
    # Get the portnr from the redis db config 
    self.iTargetRedisDbPort              = int(sc_osfile.GetConfigValue(self.cConfigpath, 'port'))
    self.cAofpath                        = os.path.join(sc_osfile.GetConfigValue(self.cConfigpath, 'dir'), 'appendonly.aof')
    self.cRdbpath                        = os.path.join(sc_osfile.GetConfigValue(self.cConfigpath, 'dir'), 'dump.rdb')
    self.tReconstructParam               = tReconstructParamIP
    self.oRedisPool                      = None
    self.oRedisClient                    = None
    self.bSwitchOffAppendonly            = False
    
    self.oLog.info('new redis restarter, parameters: workdir={0} tag={1} port={2} confpath={3} startup={4}'.format(cWorkdirIP, cRedisTagIP, iRedisPortIP, cRedisConfpathIP, cRedisStartupCommandIP))
    self.oLog.info('temp configfile: {}'.format(self.cWorkdirConfigpath))
    self.oLog.info('will restart redis db at localhost:{}'.format(self.iTargetRedisDbPort))
  
  def _RedisClientConnect(self):
    self.oRedisPool   = redis.ConnectionPool(db=0, host='localhost', port=self.iTargetRedisDbPort, socket_timeout=3000.0) # Long socket_timeout only for FT.DROP. Improve when needed. Hoping for a FT.DETACH.
    self.oRedisClient = redis.StrictRedis(connection_pool=self.oRedisPool)
    try:
      self.oRedisClient.client_setname('aof_recreate')
    except:
      # Note: Even if client_setname fails, it can still be possible to issue commands like .status(), if the server is running.
      #       The redis client tries to connect if the connection has been lost, so we don't have to depend on the command above.
      pass

  def _RedisClientDisconnect(self):
    if not (self.oRedisPool is None):
      self.oRedisPool.disconnect();
    self.oRedisPool   = None
    self.oRedisClient = None
  
  def ComposeTempConfigpath(self, cConfigpathIP, cRedisTagIP, iRedisPortIP):
  
    cTempConfigname = os.path.splitext(os.path.basename(cConfigpathIP))[0]
    cTempConfigname = '%s__%s_%s__%s.temp.conf' % (cTempConfigname, cRedisTagIP, iRedisPortIP, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    return os.path.join(self.cWorkdir, cTempConfigname) 
  
  def ConfigCopyAndUpdate(self,iPhase,oConversionPhase): 
    '''Copy the original config of a redis instance to the workdir and update the appendonly settings'''
    with open(self.cConfigpath, 'rb') as oRead, open(self.cWorkdirConfigpath, 'wb') as oWrite:
      for cLine in oRead:
        if len(cLine.rstrip()) > 0 and not cLine.startswith('#'):
          tLine = cLine.split()
          cPropertyName  = tLine[0]
          cPropertyValue = tLine[1]
          if iPhase == 0:
            if cPropertyName == 'appendonly' and cPropertyValue == 'no':
              self.bSwitchOffAppendonly = True
              cLine = 'appendonly yes\n'
            elif cPropertyName == 'auto-aof-rewrite-percentage' and int(cPropertyValue) > 0:
              cLine = 'auto-aof-rewrite-percentage 0\n'
            elif cPropertyName == 'auto-aof-rewrite-min-size' and not cPropertyValue.rstrip() == '0':
              cLine = 'auto-aof-rewrite-min-size 0\n'
          tReplace = oConversionPhase.get('config_replace_fromto',[])
          for (old, new) in tReplace:
            cLine = cLine.replace(old, new) 
        oWrite.write(cLine)
    self.oLog.info('done copying and updating target db config {} to tmp {} with success: {}'.format(self.cConfigpath, self.cWorkdirConfigpath, os.path.isfile(self.cWorkdirConfigpath)))
          
  def _ShutdownRedisDb(self):
    try:
      self._RedisClientConnect()
      self.oLog.info('SHUTDOWN NOSAVE issued for: {}:{}'.format('localhost', self.iTargetRedisDbPort))
      self.oRedisClient.execute_command('SHUTDOWN', 'NOSAVE')
    except ConnectionError:
      self._RedisClientDisconnect()
    except:
      self._RedisClientDisconnect()
      raise
    finally:
      self._RedisClientDisconnect()
  
  def WaitUntilDbIsLoaded(self):
    try:
      iRetries = 0
      while True:
        try:
          self._RedisClientConnect()  
          tInfo = self.oRedisClient.info(section='persistence')
          if tInfo['loading'] == 0:
            # done loading appendonly file, break from loop
            break
          else:
            # An optional place to send progress status to the recreate client from
            # self.oLog.info('loading %s %s %s' % (tInfo['loading'], tInfo['loading_loaded_perc'], tInfo['loading_loaded_bytes'], tInfo['loading_total_bytes']))
            time.sleep(0.5)
        except:
          self._RedisClientDisconnect()
          if iRetries < 5000:
            time.sleep(0.5)
            iRetries += 1
          else:
            break
    finally:
      self._RedisClientDisconnect()
  
  def RemoveTempConfig(self):
    if os.path.exists(self.cWorkdirConfigpath):
      os.remove(self.cWorkdirConfigpath)
    if os.path.exists(self.cWorkdirConfigpath + '.pid'):
      os.remove(self.cWorkdirConfigpath + '.pid')
  
  def RemoveAofFile(self):
    if self.cAofpath and os.path.exists(self.cAofpath):
      os.remove(self.cAofpath)
  
  def RemoveRdbFile(self):
    if self.cRdbpath and os.path.exists(self.cRdbpath):
      os.remove(self.cRdbpath)
  
  def RuntimeRestoreAppendonlyDbSettings(self): 
    '''When appendonly was switched to yes set it back to no'''
    if self.bSwitchOffAppendonly:
      try:
        self._RedisClientConnect()
        try:        
          self.oRedisClient.config_set('appendonly', 'no')
        except ConnectionError:
          pass
      finally:
        self._RedisClientDisconnect()        
  
  def RestartDbAndLoadAof(self):
    
    tRedisConversionPhases = self.tReconstructParam.get('target_redisconversions',[{},])
    
    for iPhase,oConversionPhase in enumerate(tRedisConversionPhases): 
     
      # If target redis endpoint is running: stop it    
      self._ShutdownRedisDb()
      
      # Config the original config of a redis instance to the workdir and update the appendonly settings
      self.ConfigCopyAndUpdate(iPhase,oConversionPhase)
      
      # Disconnect first, safety precaution
      self._RedisClientDisconnect()
      
      # See if redis-server is on the path
      self.oLog.info('PATH is currently: {}'.format(os.environ['PATH']))
  
      # Start new redis-server in the background. 
      try:
        self.oLog.info('launching redis-server with command: {} {}'.format(self.cStartupCommand,self.cWorkdirConfigpath))
        if platform.system() == 'Windows':
          subprocess.Popen([self.cStartupCommand, self.cWorkdirConfigpath])
        else:
          # *Must* be daemonized with close_fds on *nix systems, especially the TCP server socket (is also an fd).
          # The aof_recreate process can't be restarted (because of port busy), if we don't close_fds 
          # our child processes.
          # We also use a separate thread, to avoid defunct zombies encountered after switching to Redis 6 (alternative: use systemd/dbus python module)
          def thread_handler_start_redis(startupcommand, configpath):
            popen=subprocess.Popen([startupcommand, configpath], shell=False, close_fds=True, bufsize=-1)
            popen.wait()
          t=threading.Thread(target=thread_handler_start_redis, args=(self.cStartupCommand, self.cWorkdirConfigpath))
          t.start()
      except Exception as e:
        e.args = ('Error starting redis server. Inner message:\t{}. cStartupCommand="{}" cWorkdirConfigpath="{}"'.format(e.args[0],self.cStartupCommand,self.cWorkdirConfigpath),)
        raise e
    
      # Launching redis takes a short time anyway, so just wait a small amount of time (precaution).
      time.sleep(2.5)
      
      # Loading an appendonly file can take some time
      self.WaitUntilDbIsLoaded()
      
      # Special Conversions
      oFtDrops = oConversionPhase.get('ft.drops', None)
      if oFtDrops:
        try:
          self._RedisClientConnect()  
          for cDrop in oFtDrops:
            cEval='''return redis.call('FT.DROP', '{}')'''.format(cDrop)
            try:
              self.oLog.info('EVAL: {}'.format(cEval))
              self.oLog.info(self.oRedisClient.eval(cEval, 0))
            except Exception as e:
              self.oLog.error(e.message)
        finally:
          self._RedisClientDisconnect()
      
      # Special Conversions - save to rdb in between
      if iPhase + 1 < len(tRedisConversionPhases):
        try:
          self._RedisClientConnect()
          try:
            self.oLog.info('Saving rdb to {} for next conversion phase'.format(self.cRdbpath))
            self.oLog.info(self.oRedisClient.save())
          except Exception as e:
            self.oLog.error(e.message)
        finally:
          self._RedisClientDisconnect()
      elif iPhase > 0:
        self.RemoveRdbFile()

      # Cleanup
      self.RemoveTempConfig()
      self.RuntimeRestoreAppendonlyDbSettings()
      if iPhase==0 and self.bSwitchOffAppendonly:
        self.RemoveAofFile()
      

class c_aof_recreate_server_request_handler(c_base_request_handler, c_socket_packedmessage):
  '''The request handler for the redis_aof_recreate server. This class 
     will be instantiated once per established connection with a client 
  '''
  
  def ServerRequestHandler(self): 
    cResponseToClient = ''
    try:
      cReveivedMessage = self.PackedMessageRcv()
      if cReveivedMessage == 'close_connection': 
        self.PackedMessageSend('AOF funnel is closing this connection')
        self.RequestHandlerStop()
      elif cReveivedMessage == 'stop_redis_aof_recreate':
        cResponseToClient = 'Closing AOF Recreate Server'
        self.oLog.info(cResponseToClient)
        self.oServer.ServerStop()
      elif not cReveivedMessage is None and len(cReveivedMessage) > 0: 
        tRecreateRequest        = json.loads(cReveivedMessage)
        tRecreateRequest        = _AddParmsForReconstruct(tRecreateRequest)
        cRedisConfigpath        = self.oServer.oMain.GetRedisConfigpath(tRecreateRequest['target_redis_server_summary'])
        self.cAofpath           = os.path.join(sc_osfile.GetConfigValue(cRedisConfigpath, 'dir'), 'appendonly.aof') 
        self.cRdbpath           = os.path.join(sc_osfile.GetConfigValue(cRedisConfigpath, 'dir'), 'dump.rdb') 
        self.oLog.info('reconstruct AOF at filepath {}'.format(self.cAofpath)) 
        cReconstrHost           = self.oServer.oMain.tConfig['reconstructhost']
        cReconstrPort           = self.oServer.oMain.tConfig['reconstructport']
        oReconstructorExec      = c_aof_reconstruct_request_executor(self.cAofpath, cReconstrHost, cReconstrPort)
        bOK, cResponseToClient  = oReconstructorExec.RequestAndReconstruct(json.dumps(tRecreateRequest))
        if bOK:
          self.oServer.oMain.RedisRecreate(tRecreateRequest)
    except socket.error:
      self.oLog.error(traceback.format_exc())
      self.oLog.info('a client with host=%s and port=%s is disconnected' % self.tClientAddress)
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
        self.oLog.error('a client with host=%s and port=%s is disconnected' % self.tClientAddress)
        self.RequestHandlerStop()
  
class redis_aof_recreate(aof_server_common): 
  
  '''
  AOF Recreate Server: redis_aof_recreate
  
  TCP/IP SocketServer:
  
  Process requests asynchronously by creating a separate process or thread to handle each client request. 
  
  The process request AOF chunks from the redis_aof_reconstruct process and reconstructs the appendonly.aof file. 
  When the appendonly.aof file is reconstructed redis_aof_recreate automatically restarts the specified redis 
  instance to load the new appendonly.aof file. 
  
  To stop the recreate process send 'stop_redis_aof_recreate' via a socket connection. 
  The recreate process will stop accepting incoming server requests. When each request 
  thread is finished this process will then quit. -OR kill this process with kill -15
  '''
  
  def __init__(self, cConfigpathIP, bVerboseIP = False):
    aof_server_common.__init__(self)
    self.ConfigLoad(cConfigpathIP)
    self.ConfigAdapt()
    self.LoadRedisServerSummary()
    self.bVerbose              = bVerboseIP or self.HasForceVerboseFileInLogdir()
    self.xServerRequestHandler = c_aof_recreate_server_request_handler
    self.cServername           = 'AOF Recreate'
    self.tServerAddress        = (self.tConfig['recreatehost'], self.tConfig['recreateport'])
    self.oLog                  = sc_log.GetLogger(self.GetLogpath())
  
  def ConfigAdapt(self):
    '''Perform type conversion of specific config entries'''
    self.tConfig['logdir']                      = aExpandString(self.tConfig['logdir'])
    self.tConfig['target_redis_server_summary'] = aExpandString(self.tConfig['target_redis_server_summary'])
    self.tConfig['workdir']                     = aExpandString(self.tConfig['workdir'])
    self.tConfig['redisstartupcommand']         = aExpandString(self.tConfig['redisstartupcommand'])
    self.tConfig['recreatehost']                = str(self.tConfig['recreatehost'])
    self.tConfig['recreateport']                = int(self.tConfig['recreateport'])
    self.tConfig['reconstructhost']             = str(self.tConfig['reconstructhost'])
    self.tConfig['reconstructport']             = int(self.tConfig['reconstructport'])
    self.cAofpath = None
    self.cRdbpath = None

  def GetRedisConfigpath(self, cTargetRedisSummaryEntryIP):
    return aExpandString(self.tRedisServerSummary[cTargetRedisSummaryEntryIP]) 
  
  def RedisRecreate(self, tReconstructParamIP): 
    ''' Get the redis config filepath for a redis tag and port ''' 
    cRedisConfpath       = self.GetRedisConfigpath(tReconstructParamIP['target_redis_server_summary'])
    cWorkdir             = self.tConfig['workdir']
    cRedisTag            = tReconstructParamIP['source_redistag']
    iRedisPort           = int(tReconstructParamIP['source_redisport'])
    cRedisStartupCommand = self.tConfig['redisstartupcommand']
    oAofRedisDbRestarter = c_aof_redis_db_restarter(cWorkdir, cRedisTag, iRedisPort, cRedisConfpath, cRedisStartupCommand, self.oLog, tReconstructParamIP) 
    oAofRedisDbRestarter.RestartDbAndLoadAof()
  
  def LoadRedisServerSummary(self):
    '''Are the redis tag en port mapping to a redis instance config file'''
    self.tRedisServerSummary = json.loads(open(self.tConfig['target_redis_server_summary'], 'rb').read(), object_pairs_hook=OrderedDict)  
    
  def AddParmsForReconstruct(self, tRecreateRequestIP):
    return _AddParmsForReconstruct(tRecreateRequestIP)   

  def GetLogpath(self):
    cLogname = 'redis_aof_recreate_{0[recreatehost]}_{0[recreateport]}_{1}.log'.format(self.tConfig, datetime.date.today().isoformat()) 
    cLogpath = os.path.join(self.tConfig['logdir'], cLogname)
    return cLogpath

  def ServerStart(self): 
    
    self.oLog.info('Start {} with host={} port={}'.format(self.cServername, self.tConfig['recreatehost'], self.tConfig['recreateport']))
    aof_server_common.ServerStart(self)

if __name__ == '__main__':
  '''
  Always startup with 1 parameter: the path to the config file.
  '''
  
  if len(sys.argv) < 2:
    print 'Missing first argument: the filepath to the aof_recreate.json config of a specific redis instance. Look in the "examples" dir for documentation.'
    sys.exit(1)
  
  if not os.path.isfile(sys.argv[1]):
    print 'The config file {} does not exist'.format(sys.argv[1])
    sys.exit(2)
  
  if len(sys.argv) == 3:
    bVerbose = sys.argv[2] == True 
  else: 
    bVerbose = False
  
  cConfigpath = sys.argv[1]  
  
  oRedisAofRecreate = redis_aof_recreate(cConfigpath, bVerboseIP=bVerbose)
  # Register the redis_aof_roundtripcheck instance method handler to POSIX unix signal events.  
  signal.signal(signal.SIGINT,  oRedisAofRecreate.ServerStop)
  signal.signal(signal.SIGTERM, oRedisAofRecreate.ServerStop)
  # Wait for shutdown request
  oRedisAofRecreate.ServerStart()
  oRedisAofRecreate.WaitForServerStop()

  exit() # At this point stop all processing

# EOF
