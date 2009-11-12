'''
Look for duplicate publishers
'''

from freebase.api import HTTPMetawebSession, MetawebError


def main():
    session = HTTPMetawebSession('www.freebase.com')
    query = [{"t:type":"/book/publishing_company",
              "type":[], 
              "timestamp":None,
              "sort":"name",
              'id':None,
              'name':None
              }]

    response = session.mqlreaditer(query)
    last = ''
    count = 0
    dupes = 0
    for r in response:
        count += 1
        name = r.name
        if not name:
            print 'Null or missing name for %s' % (r.id)
            last = ''
        else:
            name = name.strip()
            if name == last:
                dupes += 1
                print '%d %d Duplicate name %s http://www.freebase.com/tools/explore%s' % (dupes, count, name, r.id)
            last = name        
        
if __name__ == '__main__':
    main()