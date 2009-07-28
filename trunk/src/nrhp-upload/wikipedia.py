'''
Created on Jul 24, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: AGPLv3 (contact author for other license options)
'''

import logging
import re
import time
import urllib2

from freebase.api import HTTPMetawebSession, MetawebError

from simplestats import Stats

log = logging.getLogger('wikipedia')

# Generator for intervals which give 1 sec. intervals (minus time already spent)
# (yes, this is just so I can play with generators!)
def waitTimeGenerator(interval):
    t1 = time.time() - interval
    while True:
        t2 = time.time()
        wait = interval - (t2 - t1)
        t1 = t2
        yield wait
# Global wait time generator
__waiter = waitTimeGenerator(1.0)

def fetch(wpid):
    # Wikipedia will reject requests without user agent field set
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]

    # Throttle Wikipedia reads to 1 req/sec
    wait = __waiter.next()
    if (wait > 0):
        time.sleep(wait)

    url = 'http://en.wikipedia.org/wiki/index.html?curid=' + str(wpid)
    try:
        handle = opener.open(url)
        content = handle.read()
        handle.close()
    except IOError:
        log.error('I/O error talking to wikipedia.org')
#        if handle != None:
#            handle.close()
        # TODO: Throw so our caller knows there's a problem
        content = ''
    
    return content

def queryArticle(wpids, matchString, regexs, exact):
    result = None
    quality = 'no'
    
    for wpid in wpids:
        content = fetch(wpid)        
        if content.find(matchString) >= 0:
            quality = 'exact'
            result = wpid
        elif not exact:
            for reg in regexs:
                if reg.search(content) != None:
                    if result == None:
                        result = wpid
                        quality = 'likely'
                        break
                    else:
                        print 'Found multiple candidates with Wikipedia matches'
                        print '  ', refNum, '    Normalized name ', name
                        print '  first - WP ID ', result
                        print '  new - WP ID ', wpid
                        result = None
                        break
    return result


def __test():
    regexs = [re.compile('.ational\s*.egister\s*.f\s*.istoric\s*.laces'),
          re.compile('.ational\s*.onument'),
          re.compile('.ational\s*.emorial')]
    print queryArticle(['52302'], "Jefferson", regexs, False)
    print queryArticle(['52302'], "asdfasdfasdfasdfasd", regexs, False)
    print queryArticle(['1'], "Jefferson", regexs, False)
    # TODO check results programmatically
    # TODO check elapsed time to make sure we aren't querying too fast

if __name__ == '__main__':
     __test() 