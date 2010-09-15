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
    parser.add_option('-u', '--user', dest='user', help='Freebase username', default='tfmorris')
    parser.add_option('-p', '--password', dest='pw', help='Freebase password')
    parser.add_option('-s', '--host', dest='host', help='service host', default = 'api.sandbox-freebase.com')
   
    (options, args) = parser.parse_args()

    user = options.user
    pw = options.pw
    host = options.host
    
    if not pw:
        pw = getpass.getpass()

    print 'Host: %s, User: %s' % (host, user)
    session = HTTPMetawebSession(host, username=user, password=pw)
    session.login()
    
    # Query which matches topics to be deleted    
    q = { '!pd:/base/jewlib/judaica_owner/research_collections': [{
            'id': '/m/0cbl9hh'
          }],
          'timestamp': [{
            'type':    '/type/datetime',
            'value<':  '2010-09-10',
            'value>=': '2010-09-09'
          }],
          'creator': {
            'id': '/user/frankschloeffel'
          },
          'id':            None,
          'name':          None,
          'sort':          'name',
          'ns0:timestamp': None,
          'type':          '/base/jewlib/research_collection',
          # Not already flagged for delete
          '!/freebase/review_flag/item':{'id':None,'optional':'forbidden'}
        }
    
    # Old style delete flagWrite query to add topic to the flagged for delete collection
#    wq = {'type' : '/freebase/opinion_collection',
#          'authority' : '/user/'+user,
#          'mark_for_delete': {
#            'id' : None,
#            'connect' : 'insert'
#          }
#      }
    
    # Write query to add topic to the flagged for delete collection
    wq =  {'type': '/freebase/review_flag',
           'kind': {'id': '/freebase/flag_kind/delete'},
           'item': {'id': None},
           'create': 'unless_exists'
         }
      
    result = session.mqlreaditer([q])
    
    count = 0
    for r in result:
        count += 1
        name = r.name if r.name else '*null*'
        print '\t'.join([str(count),r.id,name])
        wq['item']['id'] = r.id
        session.mqlwrite(wq)
    
    print 'Marked a total of %d topics for delete on %s' % (count, host)
    
if __name__ == '__main__':
    main()