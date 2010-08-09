'''
Remove a given type from a set of topics which match a given query.

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import FreebaseSession

write = False

def main():
    session = FreebaseSession.getSessionFromArgs();
    if write:
        session.login()
    
    q = {'t1:type':'/book/author',
         't2:type':'/people/person',
         'id':None,
         'name~=':'^the',  # services, firm, limited, team,associates, organization, agency, project, museum, galleries, bureau, ministry, collection, books, program[me], magazine, committee, department, foundation, staff, institute, group, commission, university, publications,library,society, association
         'name':None
        }
    q2 = {'t1:type':'/geography/geographical_feature',
         't2:type':'/geography/geographical_feature_category',
         'id':None,
         'name':None
        }
    
    # Write query to add topic to the flagged for delete collection
    wq = {'id':None,
          'type' : {'id':'/people/person',
                    'connect' : 'delete'}
          }
      
    result = session.mqlreaditer([q])
    
    count = 0
    for r in result:
#        if r.name.endswith(' INC'):
         count += 1
         print '\t'.join([str(count),r.id,r.name])
         if write:
             wq['id'] = r.id
             result = session.mqlwrite(wq)
             print '\t'.join([str(count),r.id,result.type.connect])
             n = r.name
#            if n[-1] == '.':
#                n = n[:-1]
#                q = {'id':r.id,'name':{'value':n,'lang':'/lang/en','connect':'update'}}
#                result = session.mqlwrite(q)

    
    print 'Total of %d topics on %s' % (count, session.service_url)
    
if __name__ == '__main__':
    main()
