'''
Program to count newly created topics of a given type (or which match a query)
for a time period.  We start with the desired interval and sub-divide if 
necessary due to timeouts during the counting process.

As configured, this show the dramatic spike in new book authors from the
initial OpenLibrary author load.

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import sys
from datetime import datetime, timedelta
from optparse import OptionParser
import getpass
import logging
from freebase.api import HTTPMetawebSession,MetawebError

def count(session, query, start_time, end_time):
    query["timestamp>="] = start_time.isoformat()
    query["timestamp<="] = end_time.isoformat()
    try:
        result = session.mqlread(query)
        # Uncomment the following line to see how small the interval got before
        # the query succeeded
#        print "\t".join(["",start_time.isoformat(),end_time.isoformat(),str(result)])
        return result
    except MetawebError, e:
        if e.message.find('/api/status/error/mql/timeout') < 0:
            raise e
        # TODO We should really check for runaway recursion in pathological cases
        total = 0
        slices = 4
        interval = (end_time - start_time) / slices
        for i in range(0,slices-1):
            t1 = start_time + i * interval
            t2 = t1 + interval
            total += count(session,query,t1,t2)
        return total

def main():
    start_year = 2006
    end_year = 2010
    types = ["/book/author","/book/book","/book/book_edition"]
    parser = OptionParser()
    parser.add_option("-s", "--host", dest="host", help="service host", default = 'www.freebase.com')   
    (options, args) = parser.parse_args()

    host = options.host

    print 'Host: %s' % host
    session = HTTPMetawebSession(host)

    q = {"type":"/book/author",
          "timestamp>=" : "2009-05-06T12:00",
          "timestamp<=" : "2009-05-06T18:00",
#          "creator":[{"type":"/type/attribution",
#                      "creator":"/user/book_bot"}],
          "return":"count"
          }


    oneday = timedelta(1)
    oneweek = 7 * oneday
    sixhours = oneday / 4
    
    # TODO Analyze date type added vs date topic created
    print '\t\t','\t'.join([types[i] for i in range(0,len(types))])
        
    for year in range(start_year, end_year):
        for month in range(1,13):
            t1 = datetime(year, month, 1)
            t2 = t1 + timedelta(30)
            c=[]
            for i in range(0,len(types)):
                q['type'] = types[i]
                c.append(count(session,q,t1,t2))
            args = [str(year), str(month)]
            args.extend([str(c[j]) for j in c])
            print "\t".join(args)

 
if __name__ == '__main__':
    main()