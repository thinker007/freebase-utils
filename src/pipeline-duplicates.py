'''
Created on Aug 3, 2009

@author: Tom Morris <tfmorris@gmail.com>
'''

from freebase.api import HTTPMetawebSession, MetawebError

def main():
    session = HTTPMetawebSession('www.freebase.com')
    since = "2009-10-01"
    query = [{"type":"/pipeline/merge_task",
              "timestamp":None,
              "t:timestamp>":since,
              "sort":"-timestamp",
              'id':None,
              "right_guid":None,
              "left_guid":None,
              "/pipeline/task/status":None,
              "limit":7000
              }]
    response = session.mqlread(query)
    
    mergeTasks = dict()
    dupes = 0
    for r in response:
        tuple = [r['left_guid'],r['right_guid']]
        key = ' '.join(sorted(tuple))
        if key in mergeTasks:
            dupes += 1
            print 'Duplicate pairs for GUIDs %s in tasks %s and %s' % (key,mergeTasks[key],r['id'])
        else:
            mergeTasks[key] = r['id']

    query = [{"type":"/pipeline/delete_task",
              "timestamp":None,
              "t:timestamp>" : since,
              "sort":"-timestamp",
              'id':None,
              "delete_guid":None,
              "/pipeline/task/status":None,
              "limit":9000
              }]
    response = session.mqlread(query)

    print 'Total merge dupes: %s' % dupes
    print
    
    deleteTasks = dict()
    dupes = 0
    for r in response:
        key = r['delete_guid']
        if key in deleteTasks:
            dupes += 1
            print 'Duplicate GUID %s in delete tasks %s and %s' % (key,deleteTasks[key],r['id'])
        else:
            deleteTasks[key] = r['id']
    print 'Total delete dupes: %s' % dupes
                   
if __name__ == '__main__':
    main()