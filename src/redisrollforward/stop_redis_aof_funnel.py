import socket
import struct
import json
import sys
import os
import traceback


'''
Stop redis_aof_funnel script 
'''

class c_socket(object): 
  
  def __init__(self, cHostIP, iPortIP):  
    '''Constructor
       Initialize the TCP/IP socket to communicate with the aof_funnel server instance
    '''
    self.oSocket         = None
    self.tServerAdress   = (cHostIP, iPortIP)
    self.bIsConnected    = False
    self.iMaxSendAttemps = 2 
  
  def Connect(self):
    '''Establish a connection with the aof_funnel server'''

    self.oSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.oSocket.setblocking(True)
    try:
      self.oSocket.connect(self.tServerAdress)
      self.bIsConnected = True
    except:
      self.bIsConnected = False
      self.Close()

  def Close(self):
    if not self.oSocket is None: 
      self.oSocket.close()

  def SendMessage(self, cMessageIP):
    ''' Prefix each message with a 4-byte length (network byte order) '''
    
    cMessage = struct.pack('>I', len(cMessageIP)) + cMessageIP
    self.oSocket.sendall(cMessage)

  def ReceiveMessage(self):
    ''' Read message length and unpack it into an integer '''
    
    cRawMessageLength = self.ReceiveAll(4)
    if not cRawMessageLength:
        return None
    iMessageLength = struct.unpack('>I', cRawMessageLength)[0]
    # Read the message data
    return self.ReceiveAll(iMessageLength)

  def ReceiveAll(self, iBytesIP):
      ''' Helper function to recv n bytes or return None if EOF is hit '''
      
      cData = ''
      while len(cData) < iBytesIP:
          cPacket = self.oSocket.recv(iBytesIP - len(cData))
          if not cPacket:
              return None
          cData += cPacket
      return cData


if __name__ == '__main__':
  
  '''
  Always start this script with 1 parameter: the path to the config file.
  '''

  if len(sys.argv) < 2:
    print 'Missing first argument: the filepath to the aof_read.json config of a specific redis instance. Look in the "examples" dir for documentation.'
    sys.exit(1)
  
  if not os.path.isfile(sys.argv[1]):
    print 'The config file {} does not exist'.format(sys.argv[1])
    sys.exit(2)
  
  cConfigpath = sys.argv[1]
  tConfig     = json.loads(open(cConfigpath).read(), object_pairs_hook=dict)   
  cFunnelHost = tConfig['funnelhost']
  iFunnelPort = int(tConfig['funnelport'])
  try: 
    oSocket     = c_socket(cFunnelHost, iFunnelPort)
    oSocket.Connect()
    oSocket.SendMessage('stop_redis_aof_funnel')
    print oSocket.ReceiveMessage()
    oSocket.Close()
  except Exception: 
    print 'Failed to send a stop instruction to redis_aof_funnel'
    print 'Is redis_aof_funnel running or properly connected?'
    print 'Funnel host', cFunnelHost, 'Funnel port', iFunnelPort 
    print 'ps -ef | grep funnel'
    print
    print traceback.format_exc()
  
#EOF
      