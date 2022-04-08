'''
AOF Continuous Backup Roundtrip Check. 
'''

import os
import sys
import time
import redis
import traceback
import signal

from lz4 import decompress
from datetime import datetime
from datetime import date

from amber.msroot.msmetrics.logic_xu.c_msmetrics_influxdbclient_bulk_xu import c_influxdbclient_bulk

from redisrollforward.common.redis_aof_common import sc_log
from redisrollforward.common.redis_aof_common import GenDateRange
from redisrollforward.common.redis_aof_common import aof_common
from redisrollforward.common.redis_aof_common import GetGUID

INTERVAL_TIME_SEC  = 600 # 10 minutes  
WAITTIME_CHECK_SEC = 60  # 1  minute 

class c_status:
  def __init__(self):
    self.bVerbose           = False
    self.bPause             = False
    self.bInfluxPause       = False
    self.bShutdownRequested = False 
      
class c_aof_roundtripcheck_client(aof_common): 
  
  def __init__(self, oLogIP, oStatusIP, cRedisHostIP, iRedisPortIP, cRedisTagIP, cAofBackupdirIP):

    self.oLog          = oLogIP
    self.oStatus       = oStatusIP
    self.bVerbose      = self.oStatus.bVerbose
    self.cRedisHost    = cRedisHostIP
    self.iRedisPort    = iRedisPortIP
    self.cRedisTag     = cRedisTagIP
    self.oRedisClient  = redis.StrictRedis(host=self.cRedisHost, port=self.iRedisPort)  
    self.cAofBackupdir = cAofBackupdirIP
    self.cGUID         = ''
    self.bWriteOk      = False
    self.oLog.info('Initializing c_aof_roundtripcheck_client for redis host \'{}\' port \'{}\' tag \'{}\' archive funnel backupdir \'{}\''
      .format(self.cRedisHost, self.iRedisPort, self.cRedisTag, self.cAofBackupdir)\
      )
    
  def WriteToRedisDb(self): 
    '''Writes a key & value pair in a remote redis db and then deletes it again in order to perform the roundtripcheck'''
    
    self.bWriteOk = False
    self.cGUID = GetGUID()
    if self.oStatus.bVerbose:
      self.oLog.info('{}: Write+delete redis key-value pair \'_roundtriptest_\'=\'{}\''.format(self.cRedisTag, self.cGUID))
    try:
      self.oRedisClient.client_setname('aof_roundtripcheck')
      self.bWriteOk = self.oRedisClient.set('_roundtriptest_', self.cGUID)
      self.oRedisClient.delete('_roundtriptest_') 
    except:
      self.oLog.error('{}: Cannot write and delete a key and value pair. Redis-server host=\'{}\' and port=\'{}\'\n{}'
        .format(self.cRedisTag, self.cRedisHost, self.iRedisPort, traceback.format_exc())
        )

  def VerifyTest(self, dtmStartIP, dtmEndIP):
    
    if not self.bWriteOk:
      self.oLog.warning('{}: VerifyTest is SKIPPED because WriteToRedisDb is not preceded!'.format(self.cRedisTag))
      return
    cDatetimeStart  = dtmStartIP.strftime('%Y-%m-%d_%H-%M-%S')
    cDatetimeEnd    = dtmEndIP.strftime('%Y-%m-%d_%H-%M-%S')   
    cMatchStartWith = '{}_{}__'.format(self.cRedisTag, self.iRedisPort) 
    if self.bWriteOk:  
      if self.oStatus.bVerbose:
          self.oLog.info('{}: Search GUID \'{}\''.format(self.cRedisTag, self.cGUID))
    # Search the earlier written GUID in the aofbackdir to confirm the roundtripcheck 
    bFound = False
    for dtIterSubFolder in GenDateRange(dtmStartIP.date(), dtmEndIP.date()):
      cSearchpath = os.path.join(self.cAofBackupdir, self.cRedisHost, str(dtIterSubFolder))
      for cRoot, dummy, tFile in os.walk(cSearchpath):
        for cFilename in tFile:
          if cFilename.startswith(cMatchStartWith): 
            cTimestamp = cFilename.split('_-_')[1].split('__')[0]
            if cTimestamp >= cDatetimeStart and cTimestamp <= cDatetimeEnd: 
              cAofchunkpath = os.path.join(cRoot, cFilename)
              bFound = self.HasTokenInAofchunk(cAofchunkpath, self.cGUID)
              if bFound: 
                if self.oStatus.bVerbose:
                  self.oLog.info('{}: Found GUID \'{}\' in \'{}\''.format(self.cRedisTag, self.cGUID, cAofchunkpath))
                return
    if not bFound: 
      self.oLog.error('{}: Could not retrieve the roundtripcheck GUID \'{}\' from the aofchunk archive. Redis-server host=\'{}\' and port=\'{}\''
        .format(self.cRedisTag, self.cGUID, self.cRedisHost, self.iRedisPort))
  
  def HasTokenInAofchunk(self, cAofchunkpathIP, cTokenIP): 
  
    cFileContent = open(cAofchunkpathIP).read()
    _, cFileExt = os.path.splitext(cAofchunkpathIP)
    if cFileExt == '.lz4': 
      cFileContent = decompress(cFileContent)
    return cTokenIP in cFileContent 
  
  def RedisConfigValidate(self):
    aof_common.RedisConfigValidate(self, self.cRedisTag, self.iRedisPort)
  
class redis_aof_roundtripcheck(aof_common): 
  
  def __init__(self, cConfigpathIP): 
      
      self.ConfigLoad(cConfigpathIP)
      self.oLog = sc_log.GetLogger(self.GetLogpath(), '[%(asctime)s] [%(levelname)s] %(message)s')
      self.oInfluxDbCli = c_influxdbclient_bulk(
        self.tConfig['influxdb_host'], 
        self.tConfig['influxdb_port'], 
        database=self.tConfig['influxdb_dbname'], 
        cRetentionPolicy=self.tConfig['influxdb_retentionpolicy'],
        cWorkdir=self.tConfig['influxdb_workdir'],
        cWorkfile=self.tConfig['influxdb_workfile'],
        bExceptionsToInfluxDb=True)
      self.oInfluxDbCli.set_logger(self.oLog)
      self.oStatus = c_status()
      self.ValidateConfig()
      self.AdaptConfig() 
      self.tTestClient = []
        
  def Start(self):
    '''Loads the Redis test client for each redis db server and starts the roundtripcheck'''  
    
    try:
      self.oLog.info('Start: Redis AOF Roundtripcheck')
      self.ConstructTestClients()
      self.StartLoop() 
      self.oLog.info('Finished: Redis AOF Roundtripcheck')
    except:  
      self.oLog.error(traceback.format_exc())
  
  def Stop(self, iSignumIP, oFrameIP):
    
    self.oLog.info('Process stop of Redis AOF Roundtripcheck')
    self.oStatus.bShutdownRequested = True
  
  def ConstructTestClients(self):
    '''Loads the Redis test clients into memory'''
    
    self.tTestClient = []
    for tTargetConfig in self.tConfig['targets']: 
      self.tTestClient.append(
        c_aof_roundtripcheck_client(
          oLogIP                 = self.oLog,   
          oStatusIP              = self.oStatus,
          cRedisHostIP           = tTargetConfig['redishost'],
          iRedisPortIP           = int(tTargetConfig['redisport']),   
          cRedisTagIP            = tTargetConfig['redistag'], 
          cAofBackupdirIP        = self.tConfig['aofbackupdir']
        )
      )
  
  def UpdateStatus(self): 
    
    self.oStatus.bVerbose = self.HasForceVerboseFileInLogdir()
    # Instead of looking to the redis db perstistence status (self.oRedis.info(section='persistence')['aof_rewrite_in_progress'] == 1)
    # we're applying a pause time window. During this time window we skip the roundtripcheck automatically.  
    self.oStatus.bPause = self.IsPauseRoundtripcheck()
    self.oStatus.bInfluxPause = self.IsInfluxPauseRoundtripcheck()
    if self.oStatus.bVerbose:
      self.oLog.info('Verbose mode: ON') 
      self.oLog.info('Roundtripcheck pause: {}'.format('ON' if self.oStatus.bPause else 'OFF'))
      self.oLog.info('Roundtripcheck influxdb pause: {}'.format('ON' if self.oStatus.bInfluxPause else 'OFF'))
      if self.oStatus.bPause:
        self.oLog.info('WriteDuringPause: {}'.format('ON' if self.bWriteDuringPausetime else 'OFF'))
  
  def StartLoop(self):    
    '''Waits for a certain interval and performs the roundtripcheck'''
    
    self.oStatus.bShutdownRequested = False 
    while not self.oStatus.bShutdownRequested: 
      self.UpdateStatus() # Note: It can take a while before this process will actually with start verbose logging!
      if not self.oStatus.bPause: 
        self.DoRoundtripCheck()
      elif self.bWriteDuringPausetime:
        self.DoRoundtripCheck(bWriteAndCheckIP=False)
      self.WaitAndSparseEvents(INTERVAL_TIME_SEC)   
  
  def DoRoundtripCheck(self, bWriteAndCheckIP=True):
    '''Write to each remote redis servers a test value and wait a period of time
       then verify if each test has succeeded
    '''
    # Validate the aof settings for each redis instance 
    for oTestClient in self.tTestClient:
      oTestClient.RedisConfigValidate()
    
    # Write to redis db
    dtmStart = datetime.now()
    fStartTime = time.time() 
    for oTestClient in self.tTestClient:
      oTestClient.WriteToRedisDb()
    fTotalWrite = time.time() - fStartTime
    
    if bWriteAndCheckIP:
      self.WaitAndSparseEvents(WAITTIME_CHECK_SEC)     
      if self.oStatus.bShutdownRequested: return # When a shutdown is requested there's no need to verify
      # Verify backup  
      fStartTime = time.time() 
      dtmEnd = datetime.now()
      for oTestClient in self.tTestClient:
        if self.oStatus.bShutdownRequested: return # When a shutdown is requested there's no need to verify 
        oTestClient.VerifyTest(dtmStart, dtmEnd)
      fTotalParse = time.time() - fStartTime  
    else:
      fTotalParse = 0.0
    
    try:
      self.WriteTimeMeasurementToInfluxDB(fTotalWrite, fTotalParse)
    except:
      self.oLog.error('WriteTimeMeasurementToInfluxDB failed (is the influxdb server down?). Discarding this round\'s metrics data, continuing the roundtrip check. {}'.format(traceback.format_exc()))
    
  def WriteTimeMeasurementToInfluxDB(self, fTotalWriteIP, fTotalParseIP):
    
    tMeasurement={ 'total_write_in_sec': fTotalWriteIP, 'total_parse_in_sec': fTotalParseIP}
    self.oInfluxDbCli.add_point(
      cMeasurementIP='aof_roundtripcheck_latency',
      tFieldsIP=tMeasurement,
      tTagsIP={ 'host': self.cRoundtripcheckHost },
      oTimestampIP=datetime.utcnow())
    if not self.oStatus.bInfluxPause:
      self.oInfluxDbCli.write_points_bulk()
    else:
      if self.oStatus.bVerbose:
        self.oLog.info('Skipped write to influxdb, because of influxdb_pausetimewindow: {}'.format(repr(tMeasurement)))
      
  def WaitAndSparseEvents(self, iWaittimeIP):
    
    # Current time from epoch + the max waittime in seconds
    iMaxWaitime = time.time() + iWaittimeIP
    while True:
      # If tPoints has data, this indicates that a previous write to InfluxDB has failed. 
      # Note: We choose to cache in-memory here, instead of the more robust os-file caching 
      #       of c_msmetrics_influxdbclient_bulk_xu.py.
      if self.oInfluxDbCli.tPoints:
        tPoints=self.oInfluxDbCli.tPoints
        try:
          self.oInfluxDbCli.write_points_bulk()
          if self.oStatus.bVerbose:
            self.oLog.info('Auto-retry write to influxdb succesful after influxdb_pausetimewindow or influxdb connection problems: {}'.format(repr(tPoints)))
        except:
          pass
      if (time.time() > iMaxWaitime) or self.oStatus.bShutdownRequested:  
        break
      time.sleep(0.1)
        
  def IsPauseRoundtripcheck(self):
    cTimeNow = datetime.now().strftime('%H:%M')
    return (cTimeNow >= self.cStartPause) and (cTimeNow < self.cEndPause) 
  
  def IsInfluxPauseRoundtripcheck(self):
    cTimeNow = datetime.now().strftime('%H:%M')
    return (cTimeNow >= self.cInfluxStartPause) and (cTimeNow < self.cInfluxEndPause) 
  
  def ValidateConfig(self):
   
    # The aofbackupdir must exist
    assert os.path.isdir(self.tConfig['aofbackupdir']), 'The aofbackupdir %s doesn\'t exist' % self.tConfig['aofbackupdir']
 
  def AdaptConfig(self):
    
    self.cRoundtripcheckHost = self.tConfig['roundtripcheckhost']
    self.cStartPause, self.cEndPause = self.tConfig['pausetimewindow'].split('-')
    # Convert pausetimewindow as a correctness check 
    time.strptime(self.cStartPause, '%H:%M'); time.strptime(self.cEndPause, '%H:%M')
    self.oLog.info('The pause time window: \'{}\' converted to cStartPause \'{}\' cEndPause \'{}\''
      .format(self.tConfig['pausetimewindow'], self.cStartPause, self.cEndPause))
    self.cInfluxStartPause, self.cInfluxEndPause = self.tConfig['influxdb_pausetimewindow'].split('-')
    # Convert influx pausetimewindow as a correctness check 
    time.strptime(self.cInfluxStartPause, '%H:%M'); time.strptime(self.cInfluxEndPause, '%H:%M')
    self.oLog.info('The influx pause time window: \'{}\' converted to cInfluxStartPause \'{}\' cInfluxEndPause \'{}\''
      .format(self.tConfig['influxdb_pausetimewindow'], self.cInfluxStartPause, self.cInfluxEndPause))
    self.bWriteDuringPausetime = self.tConfig['writeduringpausetime']
  
  def GetLogpath(self): 
    
    cLogname = 'redis_aof_roundtripcheck_{0}.log'.format(date.today().isoformat()) 
    cLogpath = os.path.join(self.tConfig['logdir'], cLogname)
    return cLogpath
  
  def HasForceVerboseFileInLogdir(self): 
    return os.path.isfile(os.path.join(self.tConfig['logdir'], 'redis_aof_roundtrip_verbose_force.trg'))
    
if __name__ == '__main__':
  '''
  Always startup with 1 parameter: the path to the config file.
  '''
  if len(sys.argv) < 2:
    print 'Missing first argument: the filepath to the aof_roundtripcheck.json config. Look in the \'examples\' dir for documentation.'
    sys.exit(1)
  
  if not os.path.isfile(sys.argv[1]):
    print 'The config file {} does not exist'.format(sys.argv[1])
    sys.exit(2)
 
  if len(sys.argv) == 3:
    bVerbose = sys.argv[2] == True 
  else: 
    bVerbose = False
   
  cConfigpath = sys.argv[1]  
  
  oRoundtripCheck = redis_aof_roundtripcheck(cConfigpath)
  # Register the redis_aof_roundtripcheck instance method handler to POSIX unix signal events.  
  signal.signal(signal.SIGINT,  oRoundtripCheck.Stop)
  signal.signal(signal.SIGTERM, oRoundtripCheck.Stop)
  # Start the roundtripcheck
  oRoundtripCheck.Start()

'''
To test:
1) Copy repo to:
Q:\ota\repo\wrkdev\tw
2) Adjust settings:
aof_roundtripcheck_srv-baspas.json
  "aofbackupdir"             : "/mnt/misc_baspas/backup/redis_aof",
  "logdir"                   : "/ota/wrktst",
  "influxdb_host"            : "srv-influxdb-live-02",
  "influxdb_port"            : 8086,
  "influxdb_dbname"          : "helloworld",
  "influxdb_retentionpolicy" : "", 
  "influxdb_workdir"         : "/ota/wrktst", 
  "influxdb_workfile"        : "measurements_aof_roundtripcheck_srv-baspas.dat", 
3) Change some source code to speed things up (optional, parsing the files takes a long time, especially over the mount):
  Let VerifyTest() just 'return', comment the rest
  INTERVAL_TIME_SEC  = 2
  WAITTIME_CHECK_SEC = 1 
4) Copy last changes to:
Q:\ota\repo\wrkdev\tw\py-redisrollforward\src\redisrollforward
5) Set up pythonpath:
. $SCRIPTS/PythonpathSet testT
PYTHONPATH=/ota/repo/wrkdev/tw/py-redisrollforward/src:${PYTHONPATH}
6) Run:
python /ota/repo/wrkdev/tw/py-redisrollforward/src/redisrollforward/redis_aof_roundtripcheck.py /ota/repo/wrkdev/tw/amber_fluxconfig/redisaof/srv-baspas/aof_roundtripcheck_srv-baspas.json
7) Check results with InfluxDB Studio
'''

# EOF
