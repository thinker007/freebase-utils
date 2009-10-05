'''
Find all locations contained in French departments which which have the
department name appended to the commune name and fix the names to match
the Freebase.com naming standards.

Created on October 3, 2009

@author: Tom Morris
'''

from freebase.api import HTTPMetawebSession, MetawebError

def main():
    session = HTTPMetawebSession('www.sandbox-freebase.com',
                                 username='tfmorris',password='password')
    session.login()
    query = {
             "type":     "/location/location",
             "containedby": [{
                              "type": "/location/fr_department",
                              "name": None              
                              }],
            "name":     [{
                          "value~=": "*\\,*",
                          "value": None,
                          "lang": "/lang/en"}],
            "id":       None
            }

    response = session.mqlreaditer([query])

    changes = 0
    skips = 0
    
    for r in response:
        depts = r.containedby
        if (len(depts) != 1):
            print 'Multiple departments' + repr(depts)
        dept_name = depts[0].name
        name = r.name[0].value
        if not name.endswith(dept_name): # and not name.endswith(' France'):
            print 'Skipping '+name + ' ' + dept_name
            skips +=1
        else:
            name = name.split(', '+dept_name)[0]
            if not name:
                print 'Bad split name : ' + name
            q = {"id":r.id,
                 "name" : {"value" : name,
                           "lang" : "/lang/en",
                           "type" : "/type/text",
                           "connect" : "update"}
                 }
            status = session.mqlwrite(q)
            if (status.name.connect != 'updated'):
                print 'Update failed ' + r.id + name + repr(status)
            changes += 1
    
    print ' '.join(('Changes: ',str(changes), 'Skips:', str(skips)))
    
if __name__ == '__main__':
    main()