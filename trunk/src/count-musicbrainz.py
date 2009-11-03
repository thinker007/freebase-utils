'''
Program to count the results of a complex query.  There are times when either
Freebase is being slow or our queries are too complex for them to be able to
complete without timing out.  Rather than trying to get the graph engine to
count them all for us before our timeslice expires, we iteratively fetch them
in manageable size chunks that won't time out and count them on the client.

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import sys
from optparse import OptionParser
import getpass
import logging
from freebase.api import HTTPMetawebSession

def main():
    parser = OptionParser()
    parser.add_option("-s", "--host", dest="host", help="service host", default = 'www.freebase.com')   
    (options, args) = parser.parse_args()

    host = options.host

    print 'Host: %s' % host
    session = HTTPMetawebSession(host)

    # This is the query that we want to run, but it's too complex to complete
    # It counts all topics which were created by the bot mwcl_musicbrainz
    # and originally had the type /music/artist, but now are untyped 
    # (i.e. are only typed with /common/topic).  As an additional check, we make
    # sure that they don't have a key linking them to a Wikipedia article.
    q1 = [{
		"type":        "/common/topic",
	  "timestamp":   None,
	  "id":          None,
	  "full:name":   None,
	  "t:type" :[],
	    "creator":   "/user/mwcl_musicbrainz",
	    "original:type": [{
		    "id": "/music/artist",
		    "link": {
		      "operation": "delete",
	          "valid":     False,
	          "timestamp": None,
	          "creator":   None
	          }
	       }],
	   "key": [{
	      "namespace": "/wikipedia/en",
	      "optional":  "forbidden"
	      }],
		"only:type": [{
			      "id":       None,
			      "key": [{
			          "namespace": "/common",
			          "optional":  "forbidden",
			          "value":     "topic"
			      }],
			      "optional": "forbidden"
	   	}],
	    "return":"count"
	   }]


	# Instead, let's simplify and do some of our filtering in Python after
	# we get the results back
    q = [{
		"type":        "/common/topic",
	  "timestamp":   None,
	  "id":          None,
	  "name":   None,
	    "creator":   "/user/mwcl_musicbrainz",
	    "original:type": [{
		    "id": "/music/artist",
		    "link": {
		      "operation": "delete",
	          "valid":     False,
	          }
	       }],
       "key": [{
          "namespace": "/wikipedia/en",
          "optional":  "forbidden"
          }],
       # Instead of filtering types, just ask for them all and filter as we count
	   "t:type" :[],
#        "only:type": [{
#                  "id":       None,
#                  "key": [{
#                      "namespace": "/common",
#                      "optional":  "forbidden",
#                      "value":     "topic"
#                  }],
#                  "optional": "forbidden"
#           }],

	   # we'll count ourselves instead of asking for a count
	   # "return":"count"
       
	   # if we still have problems with timeouts, we can lower the limit
	   # at the expense of a little additional overhead
	   "limit" : 20
	   }]
      
    result = session.mqlreaditer(q)
    
    total = 0
    matches = 0
    for r in result:
        total += 1
        # If they have types in addition to /common/topic, exclude them
        if len(r['t:type']) == 1:
            matches += 1
            print '\t'.join([str(matches),str(total),r.id,r.timestamp])
    
    print 'Matched %d topics from a total of %d on %s' % (matches, total, host)
    
if __name__ == '__main__':
    main()