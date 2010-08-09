'''
The simplest Python program for Freebase.  

Display the more recently created 10 topics with their id, name, and types.
'''

from freebase.api import HTTPMetawebSession, MetawebError

COUNT = 10

def main():
    session = HTTPMetawebSession('api.freebase.com')
    query = [{"t:type":"/common/topic",
              "type":[], # multiple types allowed, so return is a list
              "timestamp":None,
              "sort":"-timestamp", # sort in reverse order by creation time
              'id':None,
              'name':None,
              "limit": COUNT
              }]
    response = session.mqlread(query)
    
    print 'The last %d topics were : ' % COUNT
    for r in response:
        if r.name == None: # careful, it's possible to have a null name
            name = '<None>' 
        else:
            name = r.name
        types = ",".join(r.type)
        print '\t'.join([r.id, name, types])
        
        
if __name__ == '__main__':
    main()