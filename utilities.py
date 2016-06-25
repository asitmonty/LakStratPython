#!/usr/bin/env python

## Change Log
# Mar 7 2016- Added show_progress and update_bar function

import sys
import time
import re
    
#display progress on terminal window
def show_progress(counter, total_lines):  
  pcnt_completed = round(float(counter)/float(total_lines)*100.0,1)
  update_bar(pcnt_completed)

#update progress status
def update_bar(status):
    sys.stdout.write("\r%d%% completed..." % status)
    sys.stdout.flush()


def escape_sql_quotes(value):
    value = re.sub("'", "\'", value)
    return value

def show_elapsed(start_time, end_time):
    elapsed_time_seconds = end_time - start_time
    minutes = int(elapsed_time_seconds/60)
    seconds = elapsed_time_seconds % 60
    seconds_string = '{0:f}'.format(seconds)
    import re
    #seconds_string = str(seconds_float)
    reg_non_zero = re.search("[1-9]", seconds_string)
    if reg_non_zero is not None:
      if seconds < 1:
        pos = reg_non_zero.start()
        seconds_string = seconds_string[:pos+1]
      else:
        seconds_string = int(seconds)
    if (minutes > 0):
      return(" processing time : {0}m {1}s ".format(minutes, int(seconds)))
    else:
      return(" processing time : {0}s ".format(seconds_string))
    

class utilities:

  global levelMsg
  levelMsg =  {1:'TRACE', 2:'DEBUG', 3:'INFO'}
  
  def __init__(self, logLevel):
    self._logLevel = logLevel
    self._indent = 0

  def logit(self, level, msg):
    if level >= self._logLevel:
      self.inc_indent(level)
      print self.makedent() + levelMsg[level] + ':', msg
      self.dec_indent(level)


  def makedent(self):
    return ' ' * self._indent

  def inc_indent(self, level):
    self._indent = self._indent + 2*(3-level)
    
  def dec_indent(self, level):
    self._indent = self._indent - 2*(3-level)