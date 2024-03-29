'''
A super simple package of counters with two levels of categorization.

Created on Jul 26, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
'''
from collections import defaultdict

class _defaultdictint(defaultdict):
    def __init__(self):
        self.default_factory = int
        
class Stats(defaultdict):
    '''
    A super simple stats class for keeping counts of things in categories
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.default_factory = _defaultdictint

    def incr(self,cat,key):
        try:
            self[cat][key] += 1;
        except TypeError:
            self[cat][key] = 1;
            
    def dump(self):
        return "\n".join(["".join([category,":\n",self.dumpline(cdict)])
                          for category,cdict in iter(sorted(self.iteritems()))])

    def dumpline(self,cdict):
        # TODO extend to sort by count (probably in reverse) rather than key
        return "\n    ".join(["%20s :%5d" % (key,count) for key,count in iter(sorted(cdict.iteritems()))])
    
def __test():
    s = Stats()
    s.incr('cat1','item')
    s.incr('cat1','item')
    s.incr('cat1','item')
    s.incr('cat1','item2')
    s.incr('cat2','item3')

    print s.dump()

if __name__ == '__main__':
     __test() 