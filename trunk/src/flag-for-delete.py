'''
Perform mass 'flag for delete' operation on topics which were previously identified as 
Wikipedia disambiguation pages (by bfg-wex/person-disambig.py)

!USE THIS SPARINGLY!  If you adapt this application for other uses, understand that 
multiple human beings will have to review each and every topic that you flag for 
delete.  BE SURE that they really need to be deleted and consider doing them in 
chunks so as not to flood the review queue.

Created on Oct 18, 2009

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import sys
from optparse import OptionParser
import getpass
import logging
from freebase.api import HTTPMetawebSession

def main():
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", help="Freebase username", default='tfmorris')
    parser.add_option("-p", "--password", dest="pw", help="Freebase password")
    parser.add_option("-s", "--host", dest="host", help="service host", default = 'api.sandbox-freebase.com')
   
    (options, args) = parser.parse_args()

    user = options.user
    pw = options.pw
    host = options.host
    
    if not pw:
        pw = getpass.getpass()

    print 'Host: %s, User: %s' % (host, user)
    session = HTTPMetawebSession(host, username=user, password=pw)
    session.login()
    
    # Topics which have been typed as Wikipedia disambiguation pages (from BFG WEX analysis)
    # and which have no other types assigned and which are not flagged for delete already
    q = {# Only typed with /common/topic and our disambig page type
         't1:type': [{
            'i1:id!=':  '/user/tfmorris/default_domain/wikipedia_disambiguation_page',
            'i2:id!=':  '/common/topic',
            'id':       None,
            'name':     None,
            'optional': 'forbidden',
            'type':     '/type/type'
          }],
          'id':      None,
          'name':    None,
          'type':    [],
          't2:type': '/user/tfmorris/default_domain/wikipedia_disambiguation_page',
          # Not flagged for delete
          '!/freebase/opinion_collection/mark_for_delete': {
            'id':       None,
            'optional': 'forbidden'
          }
        }
    
    # Write query to add topic to the flagged for delete collection
    wq = {'type' : '/freebase/opinion_collection',
          'authority' : '/user/'+user,
          'mark_for_delete': {
            'id' : None,
            'connect' : 'insert'
          }
      }
      
    result = session.mqlreaditer([q])
    
    count = 0
    for r in result:
        count += 1
        print '\t'.join([str(count),r.id,','.join(r.type)])
        wq['mark_for_delete']['id'] = r.id
        session.mqlwrite(wq)
    
    print 'Marked a total of %d topics for delete on %s' % (count, host)
    
if __name__ == '__main__':
    main()