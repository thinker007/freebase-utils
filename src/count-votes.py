'''
Program to count votes
for a time period.  We start with the desired interval and sub-divide if 
necessary due to timeouts during the counting process.

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
    parser = OptionParser()
    parser.add_option("-s", "--host", dest="host", help="service host", default = 'api.freebase.com')   
    (options, args) = parser.parse_args()

    host = options.host

    print 'Host: %s' % host
    session = HTTPMetawebSession(host)
    q = {
          "type":         "/pipeline/vote",
#          "timestamp>=":  "2008-11",
          "timestamp":    None,
          "vote_value": None,
          "timestamp":None,
          "creator":None,
          "limit":500
        }
    q1 = {
          "type":         "/pipeline/vote",
          "timestamp>=":  "2008-11",
          "timestamp<=":  "2009-01",
          "timestamp":    None,
          "vote_value": {
            "name|=": [
              "delete",
              "keep",
              "merge",
              "skip",
              "don't merge",
              "left wins",
              "right wins"
            ],
            "optional": "forbidden"
          },
          "v:vote_value": None,
          "timestamp":None,
          "creator":None
        }

    for r in session.mqlreaditer([q]):
        vote = '<null>'
        if r.vote_value:
            vote = r.vote_value
        print "\t".join([r.timestamp, r.creator, vote])

 
if __name__ == '__main__':
    main()