'''Reads the aof file created by redis, and sends chunks to aof_funnel'''

import os
import sys
import time
import json
import math
import redis
import signal
import traceback

from lz4 import compress
from threading import RLock
from threading import _Event
from datetime  import datetime

from amber.msroot.msutil.logic.sc_msstring_expandosenv import sc_msstring_expandosenv

from redisrollforward.common.redis_aof_common import aof_common
from redisrollforward.common.redis_aof_common import sc_osfile
from redisrollforward.common.redis_aof_common import sc_log
from redisrollforward.common.redis_aof_common import c_thread
from redisrollforward.common.redis_aof_common import c_socketclient

from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError

xExpandString=sc_msstring_expandosenv.ExpandString

MAXBYTESIZETOSEND = 25000000 # 25 MB

class c_suspend_thread(_Event):
  
  def __init__(self):
    self.bLogfileReleased = False
    _Event.__init__(self)
  
  def wait(self):
    while self.isSet():
      time.sleep(0.1) 
  
  def WaitForLogfileRelease(self):
    self.bLogfileReleased = False
    while not self.bLogfileReleased:
      time.sleep(0.1)  

class c_aof_timestamps(object):
  
  def __init__(self, tConfigIP):
    '''When a previous aof_read instance unexpectedly quits, 
       get the "LastSend" "20150318-15:50:33" timestamp from 
       aof_read_status.dat
    ''' 
    
    self.oLock               = RLock()
    self.tStamps             = { 'Filesize': 0, 'LastSend': '', 'AmountSent': 0, 'TotalSizeChunk': 0 }
    self.cStatusLogpath      = os.path.join(tConfigIP['workdir'], 'aof_read_status.dat') 
    self.cWorkpath           = os.path.join(tConfigIP['workdir'], 'aof_read_timestamps.dat') 
    self.cWorkpathBackup     = '%s.prev' % self.cWorkpath
    self.LoadState()
    
  def GetTimestamp(self, cKeyIP):
  
    with self.oLock:
      return self.tStamps[cKeyIP]  
  
  def PutTimestamp(self, cKeyIP, cValueIP):
  
    with self.oLock:
      self.tStamps[cKeyIP]=cValueIP  
  
  def PersistState(self):
    '''Write state to aof_read_status.dat'''
    
    cFormatPhrase = (
        'LastSend {0[LastSend]}\n'
        'AmountSent {0[AmountSent]}\n'
        'TotalSizeChunk {0[TotalSizeChunk]}\n'
      )
    with open(self.cStatusLogpath, 'wb') as oFile:
      oFile.write(cFormatPhrase.format(self.tStamps))
      
  def LoadState(self):
    '''Load state from aof_read_status.dat'''
    
    if os.path.isfile(self.cStatusLogpath):
      with open(self.cStatusLogpath, 'rb') as oFile:
        for cLine in oFile:         
          cLine = cLine.rstrip()
          if cLine.startswith('LastSend'):
            self.tStamps['LastSend'] = cLine.split()[1].rstrip()
          if cLine.startswith('AmountSent'):
            self.tStamps['AmountSent'] = int(cLine.split()[1].rstrip())
          if cLine.startswith('TotalSizeChunk'):
            self.tStamps['TotalSizeChunk'] = int(cLine.split()[1].rstrip())

  def AllAofChunksSentToFunnel(self):
    
    return self.GetTimestamp('Filesize') == os.stat(self.cWorkpath).st_size

  def BackupTimestampLogfile(self): 
    
    with self.oLock: 
      # remove the old backup file (if there's one already)
      if os.path.isfile(self.cWorkpathBackup): 
        os.remove(self.cWorkpathBackup)
      # rename timestamp log file  
      if os.path.isfile(self.cWorkpath):  
        os.rename(self.cWorkpath, self.cWorkpathBackup)

  def ResetTimestampLogFile(self): 
    
    with open(self.cWorkpath, 'wb') as oFile:
      oFile.write('')      

  def GetLatestTimestampFromFile(self): 
    
    try: 
      with open(self.cWorkpath, 'rb') as oFile:
        oFile.seek(-2, 2)            
        # read until EOL
        while oFile.read(1) != "\n": 
          oFile.seek(-2, 1)       
        cLastLine = oFile.readline()
        tLastLine = cLastLine.split()
        if len(tLastLine) == 4: 
          iFilesize = int(tLastLine[2]) 
        else: 
          iFilesize = 0
    except: 
      iFilesize = 0
    finally:
      return iFilesize

class c_aof_read(object):
  '''Follows the appendonly.aof, logs filesize over time and periodically performs a bgrewriteaof''' 
  
  def __init__(self, oRedisAofReadIP, oTimestampsIP, oWaitThreadIP, oLogIP, bVerboseIP, oRedisIP, iWaittimeIP = 1.0):

    self.oRedisAofRead      = oRedisAofReadIP
    self.oTimestamps        = oTimestampsIP
    self.oWaitThread        = oWaitThreadIP
    self.oLog               = oLogIP
    self.bVerbose           = bVerboseIP
    self.iWaittime          = iWaittimeIP
    self.iPreviousFilesize  = self.oTimestamps.GetLatestTimestampFromFile()
    self.StopClientIfAofFilesizeIsSmallerThanPrevious(self.iPreviousFilesize)
    self.oPreviousTime      = datetime.now().time()
    self.oBGRewriteAoftime  = datetime.strptime(self.oRedisAofRead.tConfig['bgrewriteaoftime'],'%H:%M:%S').time()
    self.oRedisClient       = oRedisIP 
    self.bDoBgRewriteaof    = False
    self.bShutdownRequested = False
    
  def Start(self): 

    oTimestampLogger = open(self.oTimestamps.cWorkpath, 'ab')
    while not self.bShutdownRequested: 
      # check if can do bgrewriteaof
      if not self.bDoBgRewriteaof and self.IsTimeForBgRewriteaof(): 
        self.bDoBgRewriteaof = True
      if self.bDoBgRewriteaof and self.oTimestamps.AllAofChunksSentToFunnel():
        self.oLog.info('Start bgrewriteaof')
        oTimestampLogger.close()   
        # Stop the aof_read_send thread
        self.oWaitThread.set()
        self.oWaitThread.WaitForLogfileRelease()
        self.oTimestamps.BackupTimestampLogfile()
        self.DoBgRewriteaof()
        oTimestampLogger = open(self.oTimestamps.cWorkpath, 'ab')
        self.iPreviousFilesize = 0
        # Resume the aof_read_send thread
        self.oWaitThread.clear()
        self.oLog.info('Finished bgrewriteaof')
      # log aof size info with a timestamp
      if os.path.isfile(self.oRedisAofRead.tConfig['aofpath']):
        oTimestampLogger.write('%s %s\n' % (datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), self.GetAofFileStateChangeAsString()))
        oTimestampLogger.flush()
      time.sleep(self.iWaittime)
    oTimestampLogger.close()   
    self.oLog.info('Finished \'aof_read\' analysis thread')
    
  def Stop(self): 
    self.bShutdownRequested = True  

  def GetAofFileStateChangeAsString(self): 
    
    iFilesize = os.stat(self.oRedisAofRead.tConfig['aofpath']).st_size 
    cAofFileStateChange = '%s %s %s' % (self.iPreviousFilesize, iFilesize, iFilesize - self.iPreviousFilesize)
    self.iPreviousFilesize = iFilesize
    return cAofFileStateChange

  def IsTimeForBgRewriteaof(self): 
    
    oTimeNow = datetime.now().time()
    if oTimeNow >= self.oBGRewriteAoftime and self.oPreviousTime <= self.oBGRewriteAoftime :
      bIsTimeForBgRewriteaof = True
    else:
      bIsTimeForBgRewriteaof = False
    self.oPreviousTime = oTimeNow
    return bIsTimeForBgRewriteaof 
  
  def DoBgRewriteaof(self): 
    
    try:     
      bDoneBGRewriteaof = False
      self.oRedisClient.client_setname('aof_read')
      self.oRedisClient.bgrewriteaof()
      # wait until bgrewriteaof is finished
      while not bDoneBGRewriteaof:
        time.sleep(0.05)
        bDoneBGRewriteaof = self.oRedisClient.info(section='persistence')['aof_rewrite_in_progress'] == 0
      self.bDoBgRewriteaof = False
    except Exception: 
      self.oLog.error('Error while executing a bgrewriteaof {}'.format(traceback.format_exc()))

  def StopClientIfAofFilesizeIsSmallerThanPrevious(self, iPreviousFilesizeIP):
    '''If previous filesize is larger than the actual aof size, stop this redis_aof_read instance'''
    
    if os.path.exists(self.oRedisAofRead.tConfig['aofpath']) and (iPreviousFilesizeIP > os.stat(self.oRedisAofRead.tConfig['aofpath']).st_size): # POK kan mooier! maak een aofpath data-member aan
      cErr='Unexpected file size change: The previously stored filesize is larger than the actual appendonly.aof size.'
      cInfo='Cleanup the directory {0[workdir]} before restarting this aof_read_instance!'.format(self.tConfig)
      self.oLog.error(cErr)
      self.oLog.info(cInfo)
      self.oRedisAofRead.ClientStop()
      raise OSError('{}\t{}',format(cErr,cInfo))

class c_socketclient_aofchunk_sender(c_socketclient): 
  '''This class sends the timestamp chunks (that have data) to the funnel'''
  
  def __init__(self, oRedisAofReadIP, oTimestampsIP, oWaitThreadIP, oLogIP, bVerboseIP, iWaittimeIP = 1.0): 

    c_socketclient.__init__(self, oRedisAofReadIP.tConfig['funnelhost'], oRedisAofReadIP.tConfig['funnelport'])
    self.bVerbose              = bVerboseIP
    self.bShutdownRequested    = False
    self.oRedisAofRead         = oRedisAofReadIP
    self.oTimestamps           = oTimestampsIP
    self.oWaitThread           = oWaitThreadIP
    self.oLog                  = oLogIP
    self.iWaittime             = iWaittimeIP
    self.cDatetimeBGRewriteAof = '' 
    
  def ComposeAofChunkname(self, cTimeOfMeasurementIP, iChunkIP, iTotalChunksIP): 
    '''Composes the archive file name i.e: dwan_6379__2015-03-13_23-46-59_-_2015-03-13_23-51-44__000001_000001'''
    
    cFormatPhrase = '{0[redistag]}_{0[redisport]}__{1}_-_{2}__{3:06d}_{4:06d}'
    return cFormatPhrase.format(self.oRedisAofRead.tConfig, self.cDatetimeBGRewriteAof, cTimeOfMeasurementIP, iChunkIP, iTotalChunksIP) 
  
  def GetTimestamp(self, oTimestampsFileIP):
    
    cInfoTimestamp = oTimestampsFileIP.readline().rstrip()
    tInfoTimestamp = cInfoTimestamp.split()
    iFilePointer   = oTimestampsFileIP.tell()
    bIsAllSent     = len(cInfoTimestamp) == 0
    return cInfoTimestamp, tInfoTimestamp, iFilePointer, bIsAllSent
  
  def ResetTimestampsFile(self, oTimestampsFileIP):
    
    oTimestampsFileIP.close()
    self.oWaitThread.bLogfileReleased = True
    # Wait until the other thread has finished the bgrewriteaof
    self.oWaitThread.wait()
    oTimestampsFile = open(self.oTimestamps.cWorkpath, 'rb')
    iFilePointer    = 0
    return oTimestampsFile, iFilePointer
  
  def Start(self): 
    
    # Check if the appendonly.aof analysis thread produced a timestamp file
    if not os.path.exists(self.oTimestamps.cWorkpath): 
      # The other thread hasn't produced a timestamp file yet. We wait a second (quick fix, but this is good enough. Could of course be implemented by locks or such).
      self.oLog.warning('The aof_read analysis thread hasn\'t produced a timestamp file yet. Wait a second.')
      time.sleep(1.0)
    
    try: 
      oTimestampsFile = open(self.oTimestamps.cWorkpath, 'rb')
      iFilePointer    = 0
      while not self.bShutdownRequested: 
        
        oTimestampsFile.seek(iFilePointer)
        cInfoTimestamp, tInfoTimestamp, iFilePointer, bIsAllSent = self.GetTimestamp(oTimestampsFile)
        
        if not bIsAllSent and len(tInfoTimestamp) == 4:
          # Send aof chunk to redis_aof_funnel via a socket
          if not self.SendAofChunkToFunnel(tInfoTimestamp, iFilePointer): 
            # Set the file pointer to the previous byte position
            iFilePointer -= len(cInfoTimestamp) + 1 
        
        if bIsAllSent:
          self.oTimestamps.PutTimestamp('Filesize', os.stat(self.oTimestamps.cWorkpath).st_size)
        # The other thread started a bgrewriteaof
        if self.oWaitThread.isSet(): 
          # Release the aof_read_timestamps logfile
          oTimestampsFile, iFilePointer = self.ResetTimestampsFile(oTimestampsFile)
        
        # A remote process placed a clientdisconnect_pidno.trg file. Only 
        if os.path.isfile(os.path.join(self.oRedisAofRead.tConfig['workdir'], 'clientdisconnect_%s.trg' % os.getpid())):
          self.ClientSocketClose() 
        
        if bIsAllSent:
          time.sleep(self.iWaittime)

    except:
      self.oLog.error('Error in send thread. Restart the redis_aof_read instance to continue. {}'.format(traceback.format_exc()))
    
    finally:
      self.oLog.info('Finishing c_socketclient_aofchunk_sender thread')
      self.oLog.info('Inform the Aof Funnel to close the socket connection')
      self.ClientSocketClose() 
      self.oLog.info('Finished c_socketclient_aofchunk_sender thread')
  
  def Stop(self): 
    self.bShutdownRequested = True 
  
  def SendAofChunkToFunnel(self, tInfoTimestampIP, iFilePointerIP):
    
    cTimestampLastSend = self.oTimestamps.GetTimestamp('LastSend')
    cTimestampDatetime = tInfoTimestampIP[0]
    iStartBytePosition = int(tInfoTimestampIP[1])
    iFilesize          = int(tInfoTimestampIP[2])
    iBytesToSend       = int(tInfoTimestampIP[3])
    bRetOK             = False
    
    # Determine datetime bgrewriteaof 
    if (iStartBytePosition == 0) and (iFilesize == iBytesToSend):
      self.cDatetimeBGRewriteAof = cTimestampDatetime
    
    if iBytesToSend > 0 and cTimestampDatetime >= cTimestampLastSend: 
    
      # Current timestamp is LastSend assign the last known bytes that are sent
      if cTimestampDatetime == cTimestampLastSend:
        iBytesSent = self.oTimestamps.GetTimestamp('AmountSent')
        bRetOK     = iBytesSent == iBytesToSend
      else:
        iBytesSent = 0
      
      # Calculate the amount of chunks a timestamp slice contains
      iTotalChunks = int(math.ceil(iBytesToSend /  float(MAXBYTESIZETOSEND))) 
      
      while not self.bShutdownRequested and iBytesSent < iBytesToSend: 
        
        if iBytesToSend - iBytesSent - MAXBYTESIZETOSEND < 0: 
          iChunksizeToSend = iBytesToSend - iBytesSent   
        else: 
          iChunksizeToSend = MAXBYTESIZETOSEND
       
        iEndBytePosition  = iBytesSent + iChunksizeToSend
        iChunk            = int(math.ceil(iEndBytePosition / float(MAXBYTESIZETOSEND)))
        cAofSection       = sc_osfile.GetFileSectionAsString(self.oRedisAofRead.tConfig['aofpath'], iStartBytePosition + iBytesSent, iChunksizeToSend)
        bCompressToFunnel = self.oRedisAofRead.tConfig['compresstofunnel']
        
        if bCompressToFunnel == True: 
          cAofSection = compress(cAofSection)
        tAofBackupInfo = {
          'aof_read_backup_filename'     : self.ComposeAofChunkname(cTimestampDatetime, iChunk, iTotalChunks),        
          'aof_read_backup_filebytesize' : len(cAofSection),       
          'data_compressed'              : bCompressToFunnel   
        }
        
        if self.bVerbose:
          self.oLog.info('Send %s' % tAofBackupInfo['aof_read_backup_filename'])
        
        # Send data through a socket to a redis_aof_funnel instance
        bRetOK, cFunnelResponse = self.ClientSocketSendToServer('{0}\n\x80\n{1}'.format(json.dumps(tAofBackupInfo), cAofSection))
        
        if bRetOK:
          iBytesSent += iChunksizeToSend
          bRetOK      = iBytesSent == iBytesToSend
          # status snapshot file with 'LastSend cTimestampDatetime'
          self.oTimestamps.PutTimestamp('LastSend', cTimestampDatetime)
          self.oTimestamps.PutTimestamp('AmountSent', iBytesSent)
          self.oTimestamps.PutTimestamp('TotalSizeChunk', iBytesToSend)
          self.oTimestamps.PersistState()
          if self.bVerbose:
            self.oLog.info('Status LastSend {0} {1} {2}'.format(cTimestampDatetime, iBytesSent, iBytesToSend)) 
        else:
          if self.bVerbose:
            self.oLog.info('Transmission failed of "%s"' % tAofBackupInfo['aof_read_backup_filename'])
            self.oLog.info('Funnel response %s' % cFunnelResponse)
          time.sleep(self.iWaittime)
    else:
      bRetOK = True    
    
    return bRetOK
 
class redis_aof_read(aof_common): 
  
  def __init__(self, cConfigpathIP, bVerboseIP=False, bLogToDefaultOutputIP=False): 
    
    self.ConfigLoad(cConfigpathIP)
    self.ConfigAdapt()
    self.bShutdownRequested = False
    self.bClientStarted     = False 
    self.bVerbose           = bVerboseIP or self.HasForceVerboseFileInLogdir()
    self.oRedisClient       = redis.StrictRedis('localhost', self.tConfig['redisport'])
    self.oTimestamps        = c_aof_timestamps(self.tConfig)
    self.oLog               = sc_log.GetLogger(self.GetLogpath(), bLogToDefaultOutputIP=bLogToDefaultOutputIP)
    self.oWaitThread        = c_suspend_thread()
    self.oAofReadStatus     = c_aof_read(self, self.oTimestamps, self.oWaitThread, self.oLog, self.bVerbose, self.oRedisClient)
    self.oAofChunkSender    = c_socketclient_aofchunk_sender(self, self.oTimestamps, self.oWaitThread, self.oLog, self.bVerbose)
  
  def ConfigAdapt(self):
    '''Perform type conversion of specific config entries'''

    self.tConfig['logdir']     = xExpandString(self.tConfig['logdir']) 
    self.tConfig['workdir']    = xExpandString(self.tConfig['workdir']) 
    self.tConfig['aofpath']    = xExpandString(self.tConfig['aofpath'])
    self.tConfig['funnelhost'] = str(self.tConfig['funnelhost']) 
    self.tConfig['funnelport'] = int(self.tConfig['funnelport'])
    self.tConfig['redisport']  = int(self.tConfig['redisport'])
  
  def GetLogpath(self):

    cLogname = 'redis_aof_read_{0[redistag]}_{0[redisport]}_{1}.log'.format(self.tConfig, datetime.now().strftime('%Y-%m-%d'))
    cLogpath = os.path.join(self.tConfig['logdir'], cLogname)
    return cLogpath
  
  def ReadThreadsStart(self):
    '''
    Note:  Next to the main thread, we start two threads in non-daemon mode, so
    this functionality can be inserted into another project if needed.
    This is a design decision. If the number of threads is very important
    on your machine, it is possible to reimplement this using only two threads
    instead of three.
    '''
    self.oAofReadStatusThread = c_thread(self.oAofReadStatus)
    self.oAofReadStatusThread.start()
    self.oAofChunkSenderThread = c_thread(self.oAofChunkSender)
    self.oAofChunkSenderThread.start()
    
  def RedisConnect(self):
    '''
    RedisConnect connects to a redis db, executes a ping to request the redis db,
    aof_read waits a second & retries when 
      - When the redis db is not running 
      - When the redis db is busy loading
    fails it waits a second before trying it again.
    '''
    bPing = False
    self.oLog.info('Connect \'aof_read\' to redis db \'localhost\' {0[redisport]}'.format(self.tConfig))
    while not self.bShutdownRequested and not bPing:
      try:
        self.oRedisClient.client_setname('aof_read')
        bPing = self.oRedisClient.ping()
      except (ConnectionError, BusyLoadingError) as oError:
        if type(oError) is ConnectionError:
          self.oLog.warning('Cannot connect to redis db \'localhost\' {0[redisport]}. Please make sure that the redis db is running'.format(self.tConfig))
        elif type(oError) is BusyLoadingError: 
          self.oLog.info('Waiting for loading of redis db \'localhost\' {0[redisport]}'.format(self.tConfig))
      except Exception as oError: 
        self.oLog.error('Unexpected Error during RedisConnect:\n{}'.format(traceback.format_exc()))
        raise oError    
      time.sleep(1.0)
    self.oLog.info('Connection: OK')
  
  def ClientStart(self):

    if not self.bClientStarted:  
      self.bClientStarted = True
      self.bShutdownRequested = False
      if self.bVerbose:
        self.oLog.info('Start \'aof_read\' for redis instance \'localhost\' {0[redisport]}'.format(self.tConfig))
      self.RedisConnect()
      bValidConfig, cErrMsg = self.RedisConfigValidate(self.tConfig['redistag'], self.tConfig['redisport'])
      if not bValidConfig: # If the redis db configuration is invalid exit this process
        self.oLog.info('exit aof_read')
        sys.exit(cErrMsg)
      self.oLog.info('Start the \'aof_read\' analysis and sender threads'.format(self.tConfig))
      self.ReadThreadsStart()
    elif self.bVerbose: 
      self.oLog.info('The \'aof_read\' analysis and sender threads are already running')
  
  def OnClientStopByKill(self, iSignumIP, oFrameIP):
    self.oLog.info('Kill signal {} received'.format(iSignumIP))
    self.ClientStop()
  
  def ClientStop(self): 
    self.oLog.info('Stop client')
    self.bShutdownRequested = True      
    self.oAofChunkSender.Stop()
    self.oAofReadStatus.Stop()
    self.bClientStarted = False  
    
if __name__ == '__main__':
  
  '''
  Always startup with 1 parameter: the path to the config file.
  '''

  if len(sys.argv) < 2:
    print 'Missing first argument: the filepath to the aof_read.json config of a specific redis instance. Look in the "examples" dir for documentation.'
    sys.exit(1)
  
  if not os.path.isfile(sys.argv[1]):
    print 'The config file {} does not exist'.format(sys.argv[1])
    sys.exit(2)
  
  if len(sys.argv) == 3:
    bVerbose = sys.argv[2] == True 
  else: 
    bVerbose = False
  
  cConfigpath = sys.argv[1]

  oRedisAofBackup = redis_aof_read(cConfigpath, bVerboseIP=bVerbose)
  # Register the redis_aof_read instance method handler to POSIX unix signal events.  
  signal.signal(signal.SIGINT,  oRedisAofBackup.OnClientStopByKill)
  signal.signal(signal.SIGTERM, oRedisAofBackup.OnClientStopByKill) 
  # Start aof_read
  oRedisAofBackup.ClientStart()
  
#EOF
  