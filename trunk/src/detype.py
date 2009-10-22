'''
Remove a given type from a set of topics which match a given query.

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import logging
import FreebaseSession

write = True

def main():
    session = FreebaseSession.getSessionFromArgs();
    if write:
        session.login()
    
    q = {'type':'/sports/sports_league_season',
         'id':None,
         'name~=':'browns'
        }
    
    # Write query to add topic to the flagged for delete collection
    wq = {'id':None,
          'type' : {'id':'/sports/sports_league_season',
                    'connect' : 'delete'}
          }
      
    result = session.mqlreaditer([q])
    
    count = 0
    for r in result:
        count += 1
        print '\t'.join([str(count),r.id])
        if write:
            wq['id'] = r.id
            result = session.mqlwrite(wq)
            print '\t'.join([str(count),r.id,result.type.connect])

    
    print 'Total of %d topics on %s' % (count, session.service_url)
    
if __name__ == '__main__':
    main()