'''
Geographic utilities

Created on Feb 28, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html

'''

import logging
from math import cos, sqrt

from pyproj import Proj, transform

from FreebaseSession import FreebaseSession

_log = logging.getLogger('fbgeo')
_usStates = {}
_SUBS=[('(Independent City)',''),
       ('(Independent city)',''),
       ('St.','Saint'),
       ('Ste.','Sainte'),
       ('Twp.','Township'),
       ('Twp','Township'),
       ('Twnshp.','Township'),
       ('Twnshp','Township'),
       ('Ft.','Fort'),
       ('Mt.','Mount'),
        ]

_BEGIN_SUBS=[('N.','North'),('S.','South'),('E.','East'),('W.','West')]

def normalizePlaceName(name):
    for s in _BEGIN_SUBS:
        if name.startswith(s[0]):
            name = s[1]+name[2:]
    for s in _SUBS:
        name = name.replace(s[0],s[1])
    return name.strip()
    
    
def approximateDistance(a, b):
    '''Compute approximate local flat earth distance between two points represented by lat/long tuples'''
    milePerDegree = 60 * 1.15 # 1 nm/minute, 1 1/7 mile/nm - VERY APPROXIMATE!
    xdist = abs(a[0] - b[0]) * cos(abs(a[1]/180.0)) * milePerDegree
    ydist = abs(a[1] - b[1]) * milePerDegree
    dist = sqrt(xdist ** 2 + ydist ** 2)
    return dist

def isSwapped(a, b):
    '''Check for swapped long/lat coordinate pairs '''
    epsilon = .001
    if abs(a[0] - b[1]) < epsilon and abs(a[1] - b[0]) < epsilon:
        return True
    return False

def acre2sqkm(acre):
    return float(acre) * 0.004047 

## TODO split into geo and fbgeo ??

def parseGeocode(geocode):
    '''Parse a Freebase Geocode object into a long, lat [, elev] tuple'''

    lat = geocode['latitude']
    long = geocode['longitude']
    elev = geocode['elevation']

    # It's possible to have objects with no valid contents
    # Make sure we've got a complete lat/long pair
    if lat == None or long == None:
        return None

    # Elevation is optional
    if elev == None:
        return [float(long), float(lat)]
    else:
        return [float(long), float(lat), float(elev)] 


def queryUsStateGuids(session):
    '''Query Freebase and return dict of ids for states keyed by 2 letter state code '''
    query = [{'guid' : None,
               'id' : None,
               'name' : None,
               'iso_3166_2_code' : None,
               'iso:iso_3166_2_code~=' : '^US-*',
               'type' : '/location/administrative_division'
    }]
    results = session.fbRead(query)
    return dict([ (state['iso_3166_2_code'][3:5],state['guid']) for state in results])

def _initUsStateGuids(session):
    if not _usStates:
        _usStates.update(queryUsStateGuids(session))

def queryUsStateGuid(session, state):
    '''Return Guid for state from cache'''
    _initUsStateGuids(session)
    if state in _usStates:
        return _usStates[state]
    return None

def queryCityTownGuid(session, townName, stateGuid, countyName=None):
    '''Query Freebase for town by name and return single Id or None '''

    # special case Washington, DC since it's not really contained by itself
    if stateGuid == _usStates['DC'] and townName.startswith('Washington'):
        return _usStates['DC']
    
    results = queryCityTown(session, townName, stateGuid, countyName)
    if not results:
        # try again without county if we got no exact match
        results = queryCityTown(session, townName, stateGuid)
    if len(results) == 1:
        return results[0]['guid']
    elif len(results) == 2:
        # HACK to disambiguate misnamed CDPs until Freebase gets cleaned up
        cdp = '/location/census_designated_place'
        if cdp in results[0]['type'] and not cdp in results[1]['type']:
            result = results[1]['guid']
        elif cdp in results[1]['type'] and not cdp in results[0]['type']:
            result = results[0]['guid']
        else:
            # TODO One cause of multiple matches are city/town pairs with the same name
            # they often can be treated as a single place, so we might be able to figure
            # out a way to deal with this
            _log.error('Multiple matches for city/town '+townName+' in state '+stateGuid)
            return None
        _log.warn('Multiple matches for city/town ' + townName + ' in state ' + stateGuid +' picked nonCDP ' + result)
        return result
    return None
    

def queryCityTown(session, townName, stateGuid, countyName = None):
    '''Query Freebase and return list of ids for any matching towns '''
    '''county name is matched as leading string to account for both Suffolk and Suffolk County forms'''
    
    query = [{'guid' : None,
               'id' : None,
               'name':None,
               '/type/reflect/any_value' : [{'lang' : '/lang/en',
                                            'link|=' : ['/type/object/name',
                                                        '/common/topic/alias'],
                                            'type' : '/type/text',
                                            'value' : townName}],
               '/location/location/containedby' : {'guid' : stateGuid},
               't:type' : '/location/citytown',
               'type' : []
               }]
    if countyName:
        query[0]['cb:/location/location/containedby~='] = '^'+countyName
    result = session.fbRead(query)
    if not result:
        townName = normalizePlaceName(townName)
        query[0]['/type/reflect/any_value'][0]['value'] = townName
        result = session.fbRead(query)
        if not result:
            townName = townName.replace('Township','').strip()
            query[0]['/type/reflect/any_value'][0]['value'] = townName
            result = session.fbRead(query)
    return result

def queryCountyGuid(session, name, stateGuid):
    '''Query Freebase and return ID for matching county in state'''
    query = [{'guid' : None,
               'id' : None,
               'name|=' : [name, name + ' county'],
               '/location/location/containedby' : [{'guid' : stateGuid}],
               'type' : '/location/us_county'
               }]
    results = session.fbRead(query)
    if results != None and len(results) == 1:
        return results[0]['guid']

def queryLocationContainedBy(session, guid):
    '''Query Freebase for locations associated with our topic'''
    query = {'guid' :guid,
               'id' : None,
               'name' : None,
               'containedby' : [],
               'type' : '/location/location'
               }
    results = session.fbRead(query)
    if results != None:
        return results['containedby']
    return None

def queryGeoLocation(session, guid):
    '''Query Freebase and return Geocode object for given location (or None)'''
    query = {'guid' : guid,
               'geolocation' : {},
               'type' : '/location/location'
               }
    results = session.fbRead(query)
    if results == None or results['geolocation'] == None:
        return None
    
    geoId = results['geolocation']['id']
    query = {'id' : geoId,
             'guid' : None,
               'longitude' : None,
               'latitude' : None,
               'elevation' : None,
               'type' : '/location/geocode'
               }
    return session.fbRead(query)  
    
def addGeocode(session, topicGuid, coords):
    query = {'guid': topicGuid, 
             'type': '/location/location', 
             'geolocation': {'create': 'unless_connected', 
                             'type': '/location/geocode', 
                             'longitude': coords[0],
                             'latitude': coords[1]
                             }
             }
    if len(coords) > 2:
        query['elevation'] = coords[2]
    return session.fbWrite(query)


def updateGeocode(session, geocodeGuid, coords):
    '''Change the coordinates of an existing geocode'''
    query = {'guid': geocodeGuid, 
             'type': '/location/geocode', 
             'longitude': {'connect' : 'update', 'value' : coords[0]},
             'latitude': {'connect' : 'update', 'value' : coords[1]}
             }
    if len(coords) > 2:
        query['elevation'] = {'connect' : 'update', 'value' : coords[2]}
    return session.fbWrite(query)


def addArea(session, topicGuid, area):
    query = {'guid': topicGuid, 
             'type': '/location/location',
             # This will fail if it already exists, which is what we want
             'area': {'connect': 'insert', 'value': area}}
    return session.fbWrite(query)

def addContainedBy(session, topicGuid, containerGuids):
    query = {'guid' : topicGuid,
             'type' : '/location/location',
             'containedby' : [{'connect' : 'insert', 'guid' : g} for g in containerGuids]
             }
    return session.fbWrite(query)

def utm2lonlat(zone,east,north):
    '''Convert UTM NAD27 to long/lat'''
    # TODO make this fromUTM() making canonical internal proj/datum implicit?
    p1 = Proj(proj='utm',zone=zone,ellps='clrk66') #NAD27 uses the Clark 1866 ellipsoid
    # Google Maps & MS Live uses Mercator projection - "Web Mercator" == EPSG:3785 
    x1,y1=p1(east,north,inverse=True)
#    p2 = Proj(init='epsg:3785')
#    x,y=transform(p1,p2,x1,y1)
    return x1,y1

def test():
    tests = [('Newlin Twp.','Chester','PA'),
#             ('St. Petersburg Beach','Pinellas', 'FL'),
             ('W. Bradford Twp.','Chester', 'PA'),
             ('S. Brunswick Township','Middlesex', 'NJ'),
             ('Mt. Laurel Township','Burlington', 'NJ'),
#             ('','',''),
#             ('','',''),
#             ('','',''),
#             ('','',''),
#             ('','',''),
#             ('','',''),
             ]
    session = FreebaseSession('www.freebase.com','','')
    for t in tests:
        result =queryCityTown(session, t[0], queryUsStateGuid(session, t[2]), t[1]) 
        print t,result[0]['id'],result[0]['name']
        
if __name__ == '__main__':
    test()
    