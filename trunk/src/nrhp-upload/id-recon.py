'''
Check previously id pairs previously extracted from Wikipedia using 
BFG (bfg-wex/nrhp-ids.py) against the current Freebase graph and
note whether they are OK, missing, or have already been added, 
but for a different topic (a sign that the two topics need to be
reviewed for merging).

Expected input is a three column TSV file:

  index<tab>Wikipedia ID<tab>NRIS ID

Output format is

  total_count/missing_count/mismatch_count<space><keyword><space>NRIS ID<space>Freebase ID

where keyword is

  keyword:= missing | mismatch | merge
  
A simple grep filter will produce a file that can be used by the next stage in the pipe.

@since Jan 14, 2010
@author: Tom Morris <tfmorris@gmail.com>
@copyright Copyright 2010. Thomas F. Morris
@license: EPL V1
'''

import csv
from fileinput import FileInput

from freebase.api import HTTPMetawebSession,MetawebError

def readids(file):
    reader = csv.reader(FileInput(file),dialect=csv.excel_tab)
    count = 0
    ids = {}
    for r in reader:
        count += 1
        if count > 1:
            wpid = r[1]
            nrisid = r[2].rjust(8,'0')
            if len(nrisid) > 8 or nrisid.find('E') > 0:
                print '**skipping NRIS ID %s, wpid %s' % (nrisid, wpid) 
            else:
                ids[nrisid]=wpid
    print 'Read %d ids' % count
    return ids
            
def query(session, wpid,nrisid):
    wpkey = '/wikipedia/en_id/'+wpid
    q = {
         'id':wpkey,
         'guid':None,
         '/base/usnris/nris_listing/item_number' : None
         }
    
#    print wpid
    result = session.mqlread(q)
    if result:
        nrid = result['/base/usnris/nris_listing/item_number']
        if not nrid:
            nrid = ''
            result2 = session.mqlread({'key':[{'namespace':'/wikipedia/en_id','value':None,'optional':True}],
                                       'guid':None,
                                       '/base/usnris/nris_listing/item_number':nrisid})
            if result2:
                if result2['key'] and result2['key'][0]['value'] != wpid:
                    return 'mismatch %s %s' % (result['guid'],result2['guid'])
                else:
                    # TODO do some extra verification to make sure it's a topic we created?
                    return 'merge %s %s' % (result['guid'],result2['guid'])
            else:
                return 'missing %s %s' % (nrisid,wpkey)   
        else:
            return None
        
    
                               
def main():
    ids = readids('nris-ids.csv')
    session = HTTPMetawebSession('api.freebase.com');
    count = 0
    missing = 0
    mismatch = 0
    start = 0
    for nrid,wpid in ids.iteritems():
        count += 1
        if count >= start:
            result = query(session, wpid, nrid)
            if result:
                if result.startswith('missing'):
                    missing += 1
                elif result.startswith('mismatch'):
                    mismatch += 1
                print '%d/%d/%d %s' % (count,missing,mismatch,result)

if __name__ == '__main__':
    main()