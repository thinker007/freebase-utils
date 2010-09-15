'''
Utility to extract Math Genealogy ID numbers for mathematicians who are
in both Wikipedia and Freebase

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2009 Thomas F. Morris
@license Eclipse Public License v1
'''

from bfg_session import BfgSession
import bfg_wputil
from freebase.api import HTTPMetawebSession
from datetime import datetime
import logging

def fetchTopic(fbsession, wpid, mgid):
    query = [{'id': None,
              'name': None,
              'type': [],
              'key': [{
                       'namespace': '/wikipedia/en_id',
                       'value':     wpid
                       }],
              '/people/person/profession':[]
              }]
    topic = fbsession.mqlread(query)
    if topic:
        t = topic[0]
        name = t.name
        if not name:
            name = ''
        id = t.id
        if '/common/topic' in t.type:
            t.type.remove('/common/topic')
        if not t.type:
            t.type.append('*NONE*')
        if '/people/person' in t.type:
            t.type.remove('/people/person')
        # http://genealogy.math.ndsu.nodak.edu/id.php?id=nnnn
        print '\t'.join(['http://genealogy.math.ndsu.nodak.edu/id.php?id='+mgid, 
                         id,name,','.join(t['/people/person/profession']),','.join(t.type)]).encode('utf-8')
    return topic

                
def wpMathGenealogy(bfgSession, limit):
    ''' A generator that yields all pages with a MathGenealogy template'''
    for wpid,result in bfg_wputil.templatePages(bfgSession, 'MathGenealogy', limit, 
                                              subquery={'!wex:a/template_call' : None,
                                                        'wex:tc/value' : None}):
        yield wpid,result

                
def main ():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
    log = logging.getLogger('mathgen')
    log.setLevel(logging.DEBUG)
    log.info("Beginning scan at %s" % str(datetime.now()))

    bfgSession = BfgSession()
    fbSession = HTTPMetawebSession('http://api.freebase.com')

    idmap = {}
    pages = wpMathGenealogy(bfgSession, 30000)
    for wpid,result in pages:
        if 'wex:tc/value' in result:
            # TODO we should really be checking the metadata form {'param':'id'} to distinguish
            # from {'param':'name'} but we'll just check the data instead since we don't get metadata
            # returned from the treequery API
            for param in result['wex:tc/value']:
                try:
                    mgid = str(int(param))
                    break
                except ValueError:
                    mgid = None
                    pass

            if mgid:
                if wpid in idmap:
                    if idmap[wpid] != mgid:
                        log.error('*error* - WPID %s has two MathGen IDs %s and %s' % (wpid,idmap[wpid],mgid))
                    else:
#                        print 'duplicate entry for %s/%s' % (wpid,mgid)
                        pass
                else:
                    idmap[wpid] = mgid
    log.info('Number of results = %d' % len(idmap))
    
    for wpid in idmap:
        topic = fetchTopic(fbSession, wpid, idmap[wpid])
 
    log.info("Done at %s" % str(datetime.now()))

    
if __name__ == '__main__':
    main()

