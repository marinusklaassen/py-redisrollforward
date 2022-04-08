from os import path
from datetime import datetime


class PackageResource:
    def __enter__(self):
        class Package:
          def __init__(self):
            self.package_obj = Package()
            return self.package_obj
    def __exit__(self, ctype, value, traceback):
        self.package_obj.cleanup()


class ailogger(object):
  
  def __init__(self, logdir):
    self.logdir=logdir
  
  def __enter__(self):
    class ailog(object):
      def __init__(self, logdir):
        self.logdir=logdir
        self.open_logfile()
      def dispose(self):
        if not self._f is None: 
          self.close_logfile()
      def open_logfile(self):
        self._today=datetime.today().strftime('%Y-%m-%d')
        self._f = open(path.join(self.logdir,'ai_%s.log' % self._today),'ab')
        self.write_lognative('#','Start')
      def close_logfile(self):
        self.write_lognative('#','End')
        self._f.close()
      def switch_logfile(self):
        self.close_logfile()
        self.open_logfile()
      def write_logline(self, tag, msg):
        if not self._today == datetime.today().strftime('%Y-%m-%d'):
          self.switch_logfile()
        self.write_lognative(tag, msg)
      def write_lognative(self, tag, msg):
        stamp=datetime.now().strftime('%H:%M:%S') 
        self._f.write('%s %s %s\n' % (stamp, tag, msg))
        self._f.flush()
    self._inner = ailog(self.logdir)
    return self._inner

  def __exit__(self, ctype, value, tb):
    self._inner.dispose()
    return False
        
#EOF
