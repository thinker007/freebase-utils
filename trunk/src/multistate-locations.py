'''
Created on Sep 12, 2009

@author: tfmorris
'''
from freebase.api.session import HTTPMetawebSession

def main():
    q = {
         'type':   '/base/usnris/nris_listing',
         '/location/location/containedby': [{
                                             'type': '/location/us_state'
    }],
    'item_number': None,
    'name':   None,
    'id':     None,
    '/architecture/structure/address' : [{'state_province_region':None,
                                          'optional':True
                                          }]
    }


    session = HTTPMetawebSession('www.freebase.com')
    result = session.mqlreaditer([q])
    total = 0
    bad = 0
    for r in result:
        total += 1
        states = r['/location/location/containedby']
        state_count = len(states)
        if state_count > 1:
            bad += 1
            print '\t'.join([str(bad)+'/'+str(total),str(state_count), r.item_number, r.id, r.name])
            # Remove bad enum
            # create new topic with same name & usnris_listing type
            
        
if __name__ == '__main__':
    main()