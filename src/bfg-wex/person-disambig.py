'''
Utility to check Wikipedia Human name disambiguation pages 
against Freebase Person topics.

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2009 Thomas F. Morris
@license Eclipse Public License v1
'''

from bfg_session import BfgSession
import bfg_wputil
from freebase.api import HTTPMetawebSession
from datetime import datetime
import logging

def fetchTopic(fbsession, wpid):
    query = [{'id': None,
              'name': None,
              'type': [],
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
        print '\t'.join([topic[0].id,name,','.join(t.type)])
    return topic

                
def wpHndisPages(bfgSession, limit):
    ''' A generator that yields all the Wikipedia name disambiguation pages
    however they might be coded'''

    # First do the simple case - articles placed in the category directly
    for wpid in bfg_wputil.categoryPages(bfgSession, 'Human name disambiguation pages', limit):
        yield wpid

    # Now handle those which are included in the category indirectly through
    # the use of a {hndis} template call
    for wpid,scratch in bfg_wputil.templatePages(bfgSession, 'Hndis', limit):
        yield wpid

    # Ditto for Hndis pages needing cleanup
    for wpid,scratch in bfg_wputil.templatePages(bfgSession, 'Hndis-cleanup', limit):
        yield wpid
                
def main ():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
    log = logging.getLogger('language-recon')
    log.setLevel(logging.DEBUG)
    log.info("Beginning scan for Wikipedia disambiguation pages on Freebase at %s" % str(datetime.now()))

    bfgSession = BfgSession()
    fbSession = HTTPMetawebSession('http://www.freebase.com')
    
    pages = wpHndisPages(bfgSession, 30000)
    for wpid in pages:
        topic = fetchTopic(fbSession, wpid)
 
    log.info("Done at %s" % str(datetime.now()))

    
if __name__ == '__main__':
    main()

