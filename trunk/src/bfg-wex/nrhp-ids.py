'''
Utility to extract National Register Information System IDs for 
National Register of Historic Places listings which are
in both Wikipedia and Freebase using the BFG service to access
the Wikipedia EXtract (WEX).

A later stage of processing will user this information to see
which topics were misreconciled using previous strategies and
need to be merged (or split, but hopefully very few).

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2009 Thomas F. Morris
@license Eclipse Public License v1
'''

from bfg_session import BfgSession
import bfg_wputil
from freebase.api import HTTPMetawebSession
from datetime import datetime
import logging

def fetchTopic(fbsession, wpid, nrisid):
    query = [{'id': None,
              'name': None,
              '/base/usnris/nris_listing/item_number' : None,
              'key': [{
                       'namespace': '/wikipedia/en_id',
                       'value':     wpid
                       }]
              }]
    topic = fbsession.mqlread(query)
    if topic:
        t = topic[0]
        name = t.name
        if not name:
            name = ''
        id = t.id
        key = t['/base/usnris/nris_listing/item_number']
        if not key:
            key=''
        print '\t'.join([nrisid,key,id,name])
    return topic

                
def wpTemplateQuery(bfgSession, templateName, limit):
    ''' A generator that yields all pages with the given template'''
    # We use the old method because the tree query used by the new API makes the server
    # very unhappy with tens of thousands of results (even though that's not that many)
    for wpid,result in bfg_wputil.templatePagesOld(bfgSession, templateName, limit):
        yield wpid,result

                
def main ():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
    log = logging.getLogger('nris')
    log.setLevel(logging.DEBUG)
    log.info("Beginning scan at %s" % str(datetime.now()))

    bfgSession = BfgSession()
    fbSession = HTTPMetawebSession('http://www.freebase.com')

    idmap = {}
    serial = 0
    # TODO: Encode the template name
    pages = wpTemplateQuery(bfgSession, 'Infobox nrhp', 100000)
    for wpid,result in pages:
        serial+=1
        for r in result:
            if 'param' in r and r['param'] == 'refnum':
                try:
                    nrisid = str(int(r['o'].split(' ')[0]))
                    break
                except ValueError:
                    nrisid = None
                    pass

        if nrisid:
            if wpid in idmap:
                if idmap[wpid] != nrisid:
                    # Typically articles about Historical Districts with multiple infoboxes
                    log.error('*error* - WPID %s has two nris IDs %s and %s' % (wpid,idmap[wpid],nrisid))
                else:
#                        print 'duplicate entry for %s/%s' % (wpid,nrisid)
                    pass
            else:
                idmap[wpid] = nrisid
            print '\t'.join([str(serial), wpid, nrisid])

    log.info('Number of results = %d' % len(idmap))
            
    for wpid in idmap:
        topic = fetchTopic(fbSession, wpid, idmap[wpid])
 
    log.info("Done at %s" % str(datetime.now()))

    
if __name__ == '__main__':
    main()

