'''
Utilities for dealing with the WEX index for Wikipedia on Metaweb's BFG

Created on Oct 16, 2009

@author: Tom Morris <tfmorris@gmail.com>
@license: Eclipse Public License
'''

from bfg_session import BfgSession
from datetime import datetime
import logging

log = logging.getLogger('bfg-wputil')

def templatePages(bfgSession, template, limit):
    '''Generator which yields tuples containing the Wikipedia page IDs
    of all pages using this template. As well as the WEX template call
    block which invokes the template.
    
    NOTE: The WPIDs have the leading wexen:wpid/ stripped to make them easy
    to use with Freebase.  If using them with BFG, you'll need to prepend
    the prefix.  Template call IDs are raw e.g. wexen:tcid/6541444 
    so they can be easily used directly in subsequent BFG calls.
    
    This version uses the experimental treequery API.  If it goes away or breaks
    the previous version of this method is available as templatePagesOld.
    '''
    query = {'path':'wex-index',
             'query' : {'id':'Template:'+template,
                        '!wex:tc/template': {'!wex:a/template_call':None}
                        },
             'limit': limit
             }
    result = bfgSession.treequery(query)
    
    total = len(result['!wex:tc/template'])
    log.info('Number of pages in with Template:%s = %d', template, total)

    for r in result['!wex:tc/template']:
        wpid = r['!wex:a/template_call'][0]
        templateCall = r.id
        if wpid.startswith('wexen:wpid/'):
            wpid = wpid[len('wexen:wpid/'):]
            yield wpid,templateCall
        else:
            log.warn("Found subject which is not WPID - %s", wpid)
    
def templatePagesOld(bfgSession, template, limit):
    '''Generator which yields tuples containing the Wikipedia page IDs
    of all pages using this template. As well as the WEX template call
    block which invokes the template.
    
    NOTE: The WPIDs have the leading wexen:wpid/ stripped to make them easy
    to use with Freebase.  If using them with BFG, you'll need to prepend
    the prefix.  Template call IDs are raw e.g. wexen:tcid/6541444 
    so they can be easily used directly in subsequent BFG calls.
    '''
    # Get all calls to our template of interest
    # http://data.labs.freebase.com/bfg/index?path=wex-index&sub=&pred=wex%3Atc%2Ftemplate&obj=Template%3AHndis
    query = {'path':'wex-index',
             'sub': '',
             'pred':'wex:tc/template', #
             'obj':'Template:'+template,
             'limit': limit
             }
    result1 = bfgSession.query(query)
    total = len(result1)
    log.info('Number of pages in with Template:%s = %d', template, total)

    i = 0
    for r in result1:
        i += 1
        if i % 100 == 0:
            log.debug( "%d / %d" % (i, total))
        templateCall = r.s
        # Find inbound subject of which this is the object 
        # (ie template call section in main article which is calling template) 
        result2 = bfgSession.query({'path':'wex-index',
                                   'pred' : 'wex:a/template_call',
                                   'obj': templateCall})
        if len(result2) == 1:
            r = result2[0]
            if r.s.startswith('wexen:wpid/'):
                wpid = r.s[len('wexen:wpid/'):]
                yield wpid,templateCall
            else:
                log.warn("Found subject which is not WPID - %s", r.s)
        else:
            log.warn("Found more than one result - %s", repr(r))

def categoryPages(bfgSession, category, limit):
    query = {'path' : 'wex-index',
             'sub' : '',
             'pred' : 'wex:a/category', #
             'obj' : 'Category:'+category,
             'limit' : limit
             }
    result = bfgSession.query(query)
    total = len(result)
    log.info('Number of pages in Category:%s on wikipedia  = %d', category, total)
    for r in result:
        if r.s.startswith('wexen:wpid/'):
            wpid = r.s[len('wexen:wpid/'):]
            yield wpid
        else:
            log.warn('Non-wpid subject found %s',r.s)

def test():
    print "Beginning at %s" % str(datetime.now())
    bfgSession = BfgSession()
    
    count = 0
    start = datetime.now()
    for wpid,templateCall in templatePages(bfgSession, 'Infobox Language', 10000):
        count += 1
        if count == 1:
            first = datetime.now() - start
    end = datetime.now()
    elapsed = end - start;
    print 'Got %d results in %s.  Time to first result = %s' % (count, str(elapsed), str(first))
        
    count = 0
    start = datetime.now()
    for wpid,templateCall in templatePagesOld(bfgSession, 'Infobox Language', 10000):
        count += 1
        if count == 1:
            first = datetime.now() - start
    end = datetime.now()
    elapsed = end - start;
    print 'Got %d results in %s.  Time to first result = %s' % (count, str(elapsed), str(first))


if __name__ == '__main__':
    test()