import os
import json
import time
import struct
import socket
import logging
import traceback
import threading
import SocketServer
import uuid

from threading import Thread
from datetime import timedelta

# Function definitions: 

def GenDateRange(dtStartIP, dtEndIP): 
  '''Generator. Outputs a range of date objects sequentially'''

  dtIter = dtStartIP
  dtStep = timedelta(days=1)
  while dtIter <= dtEndIP:
        yield dtIter 
        dtIter += dtStep

def GetGUID():
  ''' Generates a random GUID'''
  return str(uuid.uuid4())

# Class definitions

class sc_osfile(object):

  @classmethod
  def GetConfigValue(cls, cFilepathIP, cKeyIP): 
    ''' Get a config value by key as string. Returns None if no value is found '''

    with open(cFilepathIP, 'rb') as oRead:
      for cLine in oRead:
        if len(cLine.rstrip()) > 0 and not cLine.startswith('#'):
          tLine        = cLine.split()
          cConfigKey   = tLine[0]
          cConfigValue = tLine[1]
          if cConfigKey == cKeyIP: 
            return cConfigValue

  @classmethod  
  def GetFileSectionAsString(cls, cInputFilepathIP, iStartBytePositionIP, iFilePortionExtentIP):   
   
    with open(cInputFilepathIP, 'rb') as oInputFile:
      oInputFile.seek(iStartBytePositionIP, 0)
      return oInputFile.read(iFilePortionExtentIP)

class sc_log(object):
  
  @classmethod
  def GetLogger(cls, cLogpathIP, cFormatIP='[%(asctime)s] [%(process)s] [%(levelname)s] %(message)s', bLogToDefaultOutputIP=False):

    oLog                 = logging.getLogger(cLogpathIP)
    cLogFormatter        = logging.Formatter(cFormatIP)
    hLogfileHandler      = logging.FileHandler(cLogpathIP)
    hLogfileHandler.setFormatter(cLogFormatter)
    oLog.hLogfileHandler = hLogfileHandler
    oLog.addHandler(hLogfileHandler)
    oLog.setLevel(logging.INFO)
    if bLogToDefaultOutputIP:
      oConsole = logging.StreamHandler()
      oConsole.setFormatter(cLogFormatter)
      oConsole.setLevel(logging.INFO)
      oLog.addHandler(oConsole)
    
    return oLog
  
class c_thread(Thread):
    
    def __init__(self, oInstanceIP):
      self.oInstance = oInstanceIP
      Thread.__init__(self)
    
    def run(self):
      self.oInstance.Start()

class c_socket_packedmessage(object):
  '''
  FREF@bc4c1880d: A small wrapper for dealing with socket connections as a client. Facilitates "PackedMessage" (Prefix each message with a 4-byte length (network byte order, big endian)). Implemented in java (amber_javacommon), python (amber_python) and python (py-redisrollforward). Keep changes to the independent source code locations in sync. 
  '''

  def PackedMessageSend(self, cMessageIP):
    ''' Prefix each message with a 4-byte length (network byte order, big endian) '''
    
    cMessage = struct.pack('>I', len(cMessageIP)) + cMessageIP
    self.oSocket.sendall(cMessage)

  def PackedMessageRcv(self):
    ''' Read message length and unpack it into an integer '''
    
    cRawMessageLength = self._ReceiveAll(4)
    if not cRawMessageLength:
        return None
    iMessageLength = struct.unpack('>I', cRawMessageLength)[0]
    # Read the message data
    return self._ReceiveAll(iMessageLength)

  def _ReceiveAll(self, iBytesIP):
    ''' Helper function to recv n bytes or return None if EOF is hit '''
      
    cData = ''
    while len(cData) < iBytesIP:
      cPacket = self.oSocket.recv(iBytesIP - len(cData))
      if not cPacket:
        return None
      cData += cPacket
    return cData

class c_socketclient(c_socket_packedmessage):
  '''
  FREF@bc4c1880d: A small wrapper for dealing with socket connections as a client. Facilitates "PackedMessage" (Prefix each message with a 4-byte length (network byte order, big endian)). Implemented in java (amber_javacommon), python (amber_python) and python (py-redisrollforward). Keep changes to the independent source code locations in sync. 
  '''
  
  def __init__(self, cHostIP, iPortIP):
    '''Initialize the TCP/IP socket'''
    
    self.oSocket         = None
    self.tServerAddress   = (cHostIP, iPortIP)
    self.bIsConnected    = False
    self.iMaxSendAttemps = 2 
    
  def ClientConnectToServer(self):
    '''Establish a connection with a server'''

    self.oSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.oSocket.setblocking(True)
    try:
      self.oSocket.connect(self.tServerAddress)
      self.bIsConnected = True
    except:
      self.bIsConnected = False
      self.Close()
  
  def Close(self):
    self.oSocket.close()
  
  def ClientSocketClose(self):
    
    if not self.oSocket is None: 
      self.SendStopConnectionRequest()  
      
  def SendStopConnectionRequest(self):
    self.PackedMessageSend('stop_connection')    

  def ClientSocketSendToServer(self, cSendDataIP): # FREF@7b08b89b1 
    
    iTransmissionAttempt = 0  
    cResponseMessage     = ''
    bPartOkay            = False

    while (not bPartOkay) and (iTransmissionAttempt < self.iMaxSendAttemps): 
      try:
        if not self.bIsConnected:  
          self.ClientConnectToServer()
        self.PackedMessageSend(cSendDataIP)
        # Receive response
        cResponseMessage = self.PackedMessageRcv()
        bPartOkay        = cResponseMessage == 'part_okay'
        if not bPartOkay: 
          iTransmissionAttempt += 1
      except socket.error as oError:
        self.bIsConnected = False
        iTransmissionAttempt += 1 
        '''Send or receive data was disallowed because the socket is not connected and no address was supplied, OR
             - An existing connection forcibly closed by the remote host'''
        if (oError.errno == 10057) or (oError.errno == 10054): 
          break
      except:  
        # Unexpected error: try sending the message again
        iTransmissionAttempt += 1
    
    return bPartOkay, cResponseMessage

class c_base_request_handler(c_socket_packedmessage):

    def __init__(self, oRequestSocketIP, tClientAddressIP, oServerIP):
      
      self.oSocket            = oRequestSocketIP
      self.tClientAddress     = tClientAddressIP
      self.oServer            = oServerIP
      self.bShutdownRequested = False
      self.oLog               = None
      self.RequestSetup()
    
    def SetLogger(self, oLogIP):
      
      self.oLog = oLogIP
    
    def ServerRequestHandlerStart(self):
      
      if not self.oLog is None: 
        self.oLog.info('Start request handler for connection host: %s port: %s' % self.tClientAddress)
      try:
        self.ServerRequestHandler()
      finally:
        self.RequestFinish()

    def RequestHandlerStop(self):
      
      self.bShutdownRequested = True
      
    def RequestSetup(self):
        pass

    def RequestFinish(self):
      
      if not self.oLog is None: 
        self.oLog.info('Remove request handler for connection host: %s port: %s' % self.tClientAddress)
      self.oServer.RemoveServerRequestHandler(self)    

class c_threaded_tcpserver(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
  
  def __init__(self, cServerNameIP, tServerAddressIP, xServerRequestHandlerIP, oMainIP, oLogIP):
    
    self.allow_reuse_address    = True
    
    self.cServerName            = cServerNameIP
    self.oMain                  = oMainIP
    self.oLog                   = oLogIP
    self.bVerbose               = False
    self.bShutdownRequested     = False
    self.tServerAddress         = tServerAddressIP
    self.tServerRequestHandler  = set()
    self.xServerRequestHandler  = xServerRequestHandlerIP
    SocketServer.TCPServer.__init__(self, tServerAddressIP, self.xServerRequestHandler)

  def finish_request(self, oRequestSocketIP, tClientAddressIP):
    """Finish one request by instantiating RequestHandlerClass."""
    
    oRequestHandler = self.xServerRequestHandler(oRequestSocketIP, tClientAddressIP, self)
    oRequestHandler.SetLogger(self.oLog)
    self.tServerRequestHandler.add(oRequestHandler)
    oRequestHandler.ServerRequestHandlerStart()

  def RemoveServerRequestHandler(self, oServerRequestHandlerIP):
    
    if oServerRequestHandlerIP in self.tServerRequestHandler:
      self.tServerRequestHandler.remove(oServerRequestHandlerIP)

  def ServerStart(self): 
    
    try:
      self.oLog.info('Start the %s TCP/IP Server with address: Host=%s Port=%s' % (self.cServerName, self.tServerAddress[0], self.tServerAddress[1])) 
      self.oServerThread        = threading.Thread(target=self.serve_forever)
      self.oServerThread.setDaemon(True)
      self.oServerThread.start()
    except Exception: 
      self.oLog.error('Failed to start %s TCP/IP Server: %s' % (self.cServerName, traceback.format_exc()))
      exit()
  
  def ServerStop(self):
    
    try: 
      self.oLog.info('Shutdown %s' % self.cServerName)
      for oServerRequestHandler in self.tServerRequestHandler: 
        oServerRequestHandler.RequestHandlerStop()
      self.bShutdownRequested = True  
      self.shutdown()  
      self.server_close()
    except Exception: 
      self.oLog.error('Failed to stop %s TCP/IP Server: %s' % (self.cServerName, traceback.format_exc()))

class aof_common(object):
  
  def ConfigLoad(self, cConfigpathIP):
    self.tConfig = json.loads(open(cConfigpathIP).read(), object_pairs_hook=dict) 
  
  def HasForceVerboseFileInLogdir(self): 
    return os.path.isfile(os.path.join(self.tConfig['logdir'], 'verbose_force.trg'))

  def RedisConfigValidate(self, cRedistagIP, iPortIP):
    '''
    aof_read depends on the follows configuration settings: 
    appendonly yes                  
    auto-aof-rewrite-percentage 0   
    auto-aof-rewrite-percentage 0   
    
    If the configuration settings are not valid the aof_read daemon process will exit.
    '''
    bOk     = False
    cErrMsg = ''
    try: 
      if self.bVerbose:
        self.oLog.info('Validating AOF configuration settings for the redis instance with tag: \'{0}\' port: {1}'.format(cRedistagIP, iPortIP))
      tDbConfig = self.oRedisClient.config_get() 
      bOk = tDbConfig['appendonly'] == 'yes' and tDbConfig['auto-aof-rewrite-percentage'] == '0' and tDbConfig['auto-aof-rewrite-min-size'] == '0'
      if bOk:
        if self.bVerbose:
          self.oLog.info('AOF configuration settings: OK' )
      else:
        cErrMsg = 'The AOF settings for the redis instance with tag \'{0}\' and port {1} are invalid.\n'.format(cRedistagIP, iPortIP) + (  
                  'Change AOF settings online & in the redis.conf to:\n' 
                  'appendonly yes                                    \n'                   
                  'auto-aof-rewrite-percentage 0                     \n'    
                  'auto-aof-rewrite-percentage 0                     \n'
                  )
        self.oLog.error(cErrMsg)
    except:  
      cErrMsg = 'Error while validating redis db configuration settings.\n{}'.format(traceback.format_exc())
      self.oLog.error(cErrMsg)
    return bOk, cErrMsg 
  
class aof_server_common(aof_common): 
  
  def __init__(self):
    self.oServer = None
    self.bShutdownRequested = False
  
  def ServerLoad(self):
      
    try:
      self.oServer = c_threaded_tcpserver(self.cServername, self.tServerAddress, self.xServerRequestHandler, self, self.oLog)
      self.oServer.socket.setblocking(True)
    except socket.error as e:
      if e.errno in (10048, 99):
        e.args = (e.args[0], 'Failed to start {0}:\t{1[1]}'.format(self.cServername, e.args))
        self.oLog.error(e.args[1])
        raise e
      else: 
        raise

  def ServerStart(self): 
    
    self.bShutdownRequested = False
    self.ServerLoad()
    self.oServer.ServerStart()    

  def ServerStop(self, iSignumIP=None, oFrameIP=None):
    
    self.bShutdownRequested = True 
    self.oLog.info('Processing ServerStop for server {}'.format(self.cServername)) 
    if not self.oServer is None: 
      self.oServer.ServerStop() 
    else: 
      self.oLog.info('Server {} is not loaded'.format(self.cServername))     
    self.oLog.info('ServerStop is finished') 
      
  def WaitForServerStop(self):
    self.oLog.info('Enable waiting for server {}'.format(self.cServername))
    while not self.bShutdownRequested:
      time.sleep(0.3)
    self.oLog.info('Done waiting for server {}'.format(self.cServername))
    
#EOF
      