'''
Fix up bad author names from Open Library load.

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import sys
from bisect import bisect_left
import codecs
from datetime import datetime, timedelta
from optparse import OptionParser
import getpass
import logging
from freebase.api import HTTPMetawebSession,MetawebError

badprefix = ['jr','sr','inc','phd','ph.d','m.d','iii']
onlyprefix = ['mr','mrs','dr','sir','dame','rev']

badwords = [
            'agency',
            'america',
            'american',
            'assembly',
            'association',
            'associates',
            'australia',
            'australian',
            'bank',
            'board',
            'britain',
            'british',
            'center',
            'centre',
            'church',
#            'citizen',
            'club',
            'collection',
            'commission',
            'committee',
            'commonwealth',
            'common-wealth',
            'council',
            'department',
            'dept.',
            'editors',
            'federation',
#            'friend',
            'foundation',
            'galleries',
            'gallery',
            'great britain',
            'institut.',
            'institute',
            'libraries',
            'laboratory',
            'library',
            'limited',
            'ltd.',
            'magazine',
            'mission',
            'member', # whitelist things which start 'member of' to catch oldie worldy anon. authors
            'museum',
#            'New York',
            'office',
            'program',
            'project',
            'publications',
            'research',
            'school',
            'schools',
            'secretariat',
            'service',
            'services',
            'society', # Not 100% because of "member of ___ society"
            'staff',
            'trust', # more for stock holders than authors
            'university',
            'and',
            '&',
            'author',
            'none',
            'on',
            'pseud',
            'unk',
            'unknown',
            'with',
            'by',
            'gmbh',
            'tbd',
            'the'
            ]
badwordsmax = len(badwords)

def badname(s):
    # TODO separate flags for compound name, corporate name, illegal characters, etc
    
    # TODO check for bad punctuation, particularly reverse parens x) yyy (z
    word = s.split()

    # Check first word for things which should only be last word
    f = word[0].strip().lower()
    if f[-1] == '.':
        f=f[:-1]
    if f in badprefix:
        return True
    if f in onlyprefix:
        del word[0]

    for w in word:
        lw = w.lower()
        if w[-1] == '.':
            w = w[:-1]
            # Look for non-initials which end in period 
            # (allow multiple initials without spaces e.g. p.g. whodehouse)
            if len(w) > 2 and w[1] != '.' and w != 'ph.d' and w != 'ste':
                return True

        b = bisect_left(badwords,lw)
        if b >= 0 and b < badwordsmax-1 and badwords[b] == lw:
            # TODO add secondary check for ambiguous words which can be personal names e.g. Service
            return True
        
    # Check against name list and kick low probability names?
    # check for all caps
    # check for place names
    return False

    
def main():
    parser = OptionParser()
    parser.add_option('-s', '--host', dest='host', help='service host', default = 'www.freebase.com')   
    (options, args) = parser.parse_args()

    host = options.host
    badwords.sort()

    print 'Host: %s' % host
    session = HTTPMetawebSession(host)

    q = {'t1:type':'/book/author',
         't2:type':'/people/person',
         'id' : None,
         'name' : None,
         'creator' : None,
#          'timestamp<=' : '2009-05-01',
#          'timestamp>=' : '2009-05-01',
#          'timestamp<=' : '2009-07-30',
#          'timestamp>=' : '2009-06-30',
#          'creator':[{'type':'/type/attribution',
#                      'creator':'/user/book_bot'}]
          }

    total = 0
    bad = 0
    for r in session.mqlreaditer([q]):
        total += 1
        if r.name and badname(r.name):
            bad += 1
            print '\t'.join([str(bad), str(total), r.id,r.name,r.creator]).encode('utf-8')

 
if __name__ == '__main__':
    print 'Starting at %s' % str(datetime.now())
    main()
    print 'Done at %s' % str(datetime.now())
