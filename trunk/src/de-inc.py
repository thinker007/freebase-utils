'''
Look for duplicate publishers
'''

from freebase.api import HTTPMetawebSession, MetawebError


def main():
    session = HTTPMetawebSession('api.freebase.com')
    query = {
      "type":"/business/company",
      'name':None,
      "id":None
    }

#    suffixes = ['inc.', 'inc', 'incorporated', 'co.', 'co', 'company']
    # TODO prefix AB, suffixes Ltd, Limited, LLC
    # TODO make this data driven from someplace?
    suffixes = ['inc', 'incorporated', 'co', 'company', 'corp', 'corporation']
    count = 0
    dupes = 0
    start = 8298 # start index to speed up restart after failure
    for suffix in suffixes:
        query['name~='] = '* ' + suffix.replace('.','\\.') + '$'
        
        response = session.mqlreaditer([query])
        for r in response:
            count += 1
            if count < start:
                continue
            name = r.name
            if not name:
                print 'Null or missing name for %s' % (r.id)
                last = ''
            else:
                n1 = name.lower().strip()
                if n1[-1] == '.':
                    n1 = n1[:-1]
                if not n1.endswith(suffix):
                    print 'No suffix match %s %s' % (name,suffix)
                else:
                    n1 = n1[:-len(suffix)]
                    n1 = n1.strip()
                    if n1[-1] == ',':
                        n1 = n1[:-1]
                    rdup = session.mqlread([{'type':'/business/company',
                                                     'name|=':[n1 + x for x in ['',
                                                                               ' inc',
                                                                               ', inc',
                                                                               ' inc.',
                                                                               ', inc.',
                                                                               ' incorporated',
                                                                               ', incorporated',
                                                                               ' co',
                                                                               ', co',
                                                                               ' co.',
                                                                               ', co.',
                                                                               ' corp',
                                                                               ', corp',
                                                                               ' corp.',
                                                                               ', corp.',
                                                                               ' corporation',
                                                                               ' company',
                                                                               ', company']],
                                                     'name':None,
                                                     'id':None}])
                    if not rdup:
                        print 'ERROR: no name match %s %s' % (n1, name)
                    else: 
                        if len(rdup)>1:
                            print 'Set of dupes:'
                            for rd in rdup:
                                dupes += 1
#                                print '  %d %d %s http://www.freebase.com/tools/explore%s' % (dupes, count, rd.name, rd.id)
                                print '\t'.join([str(dupes), str(count), rd.name, rd.id])
                        else:
                            pass
#                            print 'Unique name %s %s' % (n1,name)
    
        
if __name__ == '__main__':
    main()
