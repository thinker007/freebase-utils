'''
Simple demo of the geosearch API.

Find the 10 closest National Register of Historic Places listings which are
of National significance and within 50 km, using the most recently added
listing as a starting point for the search.

Requires a version of freebase-api greater than 1.0.3

@author: Tom Morris <tfmorris@gmail.com>
@license: EPL v1
'''

import sys
import logging
from freebase.api import HTTPMetawebSession

def main():
    session = HTTPMetawebSession('www.sandbox-freebase.com')

    # Query to find the most recent site with a geolocation
    q = [{
        "t:type":        "/base/usnris/nris_listing",
        "type": "/location/location",
      "id":          None,
      "name":   None,
      # A nested request for longitude makes this non-optional
      "geolocation":{'longitude':None},
      "timestamp": None,
      "sort": "-timestamp",
      "limit": 1
       }]
      
    result = session.mqlread(q)
    r = result[0]
    print "Using %s %s as the base location" % (r.id,r.name)

    mql_filter = [{"type" : "/base/usnris/nris_listing",
                   "significance_level":"National"
    }]
    response = session.geosearch(location=r.id,  
                                 mql_filter = mql_filter,
                                 within=50.0,order_by='distance', limit=10)
      
    for r in response.result.features:
        p = r.properties
        print "%f km id: %s name: %s" % (p['geo:distance'],p.id,p.name)
        
  
if __name__ == '__main__':
    main()