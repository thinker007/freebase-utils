'''
 Identify duplicate publisher topics.  
 Simplistic analysis currently - only finds identical name matches
 (230 of 10,500 publishers)
'''

import codecs
from freebase.api import HTTPMetawebSession, MetawebError

def normalize_publisher_name(name):
    # remove parenthetical expressions
    # remove company identifiers (Inc., Co., Ltd, Group, etc)
    # remove Publishing, Publications, Press, Books, Editions, 
    # normalize abbreviations (also '&' vs 'and')
    # split place off <publishing place> : <publisher>
    return name

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
                print '%d\t%d\tDuplicate\t%s\thttp://www.freebase.com/tools/explore%s' % (dupes, count, codecs.encode(name,'utf-8'), r.id)
            else:
                print '%d\t%d\t\t%s\thttp://www.freebase.com/tools/explore%s' % (dupes, count, codecs.encode(name,'utf-8'), r.id)
                
            last = name        
        
if __name__ == '__main__':
    main()