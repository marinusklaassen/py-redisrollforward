import json
import re


from collections import OrderedDict
from datetime import datetime

class statuslog(object):
  
  def __init__(self, cFilepathIP):
    '''Constructor
    '''
    self.tColumnProperties = None
    self.oRegexMatchDigit  = re.compile(r'\d+') 
    self.cDatetimeFormat   = '{:<21}'
    self.cFilepath         = '%s_%s.log' % (cFilepathIP, datetime.now().strftime('%Y%m%d'))
  

  @classmethod 
  def WriteSnapshotStatus(cls, tStatusIP, cFilepathIP):
    '''Write json to a snapshot file. 
       The separate snapshot file will be overwritten with each call.'''
        
    ''' write message to snapshot log '''
    with open(cFilepathIP,'wb') as oFile:
      oFile.write(json.dumps(tStatusIP))

  @classmethod 
  def GetSnapshotStatus(cls, cFilepathIP):
    '''Get json from snapshot file.'''
    
    return json.loads(open(cFilepathIP).read(), object_pairs_hook=dict)
    

  def WriteStatus(self, cMessageIP): 
    
    with open(self.cFilepath, "a") as oFile:
      oFile.write(cMessageIP)

  
  def WriteStatusLn(self, cMessageIP, bDatetimeStamp=False): 
    
    if bDatetimeStamp:
      self.WriteStatus('{0}{1}\n'.format(self.GetFormattedDatetime(), cMessageIP))
    else: 
      self.WriteStatus('{0}{1}\n'.format('', cMessageIP))

  
  def WriteStatusEmptyLn(self): 
    self.WriteStatus('\n')
    
   
  def GetFormattedDatetime(self):
    return self.cDatetimeFormat.format(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
  
  
  def GetJsonAsOrderedDict(self, cJsonIP): 
    return json.loads(cJsonIP, object_pairs_hook=OrderedDict)
  
  
  def SetColumnProperties(self, cColumnPropertiesAsJsonIP): 
    
    self.tColumnProperties = self.GetJsonAsOrderedDict(cColumnPropertiesAsJsonIP)
    
  
  def GetStatusDictAsString(self, tStatusDictIP):
 
    cStatusLn = ''
    if self.tColumnProperties is None: 
      for cStatusDictKey in tStatusDictIP.iterkeys(): 
        cStatusLn = '{0} {1}'.format(cStatusLn, tStatusDictIP[cStatusDictKey])
    else:   
      for cColumnTitle in self.tColumnProperties.keys(): 
        tOneColumnProperty = self.tColumnProperties[cColumnTitle]
        if isinstance(tOneColumnProperty, (dict)) and \
           tOneColumnProperty.has_key('format')   and \
           tStatusDictIP.has_key(cColumnTitle): 
          cColumnLn = tStatusDictIP[cColumnTitle]
        else: 
          cColumnLn = '' 
        cStatusLn += tOneColumnProperty['format'].format(cColumnLn)
        
    return cStatusLn 
   
   
  def WriteStatusColumnHeaders(self): 
    if not self.tColumnProperties is None: 
      cColumnHeaderLine = ''
      for cColumnTitle in self.tColumnProperties.iterkeys(): 
        tOneColumnProperty = self.tColumnProperties[cColumnTitle]
        if isinstance(tOneColumnProperty, (dict)) and tOneColumnProperty.has_key('format'): 
          cColumnHeaderLine += tOneColumnProperty['format'].format(cColumnTitle)  
      self.WriteStatusLn(cColumnHeaderLine, bDatetimeStamp=False)  
      self.WriteStatusLn(self.WriteHeaderSeparatorLines(), bDatetimeStamp=False)  

    
  def WriteHeaderSeparatorLines(self): 
    cColumnHeaderLine = ''
    for cColumnTitle in self.tColumnProperties.iterkeys(): 
      cColumnHeaderLine += '{s:{c}^{n}}'.format(s='',n=self.oRegexMatchDigit.search(self.tColumnProperties[cColumnTitle]['format']).group(0),c='-')
    return cColumnHeaderLine
  
  
  def WriteStatusDictToLn(self, tStatusDictIP): 
    
    if isinstance(tStatusDictIP, (dict)): 
      self.WriteStatus('{0}\n'.format(self.GetStatusDictAsString(tStatusDictIP)))
    else: 
      raise TypeError('The input to WriteStatusArrayToLn is not a dictionary {}')
    

if __name__ == '__main__':
  
  '''
  oLog = statuslog('e:/test1')
  oLog.WriteStatusLn('test')
  oLog.WriteStatusDictToLn({ 'a': 1, 'b' : 2})
  '''
#EOF
