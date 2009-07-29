'''
Geographic utilities

Created on Feb 28, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
'''

import logging
from math import cos, sqrt
#from freebase.api import HTTPMetawebSession, MetawebError

log = logging.getLogger('fbgeo')

def approximateDistance(a, b):
    '''Compute approximate local flat earth distance between two points represented by lat/long tuples'''
    milePerDegree = 60 * 1.15 # 1 nm/minute, 1 1/7 mile/nm - VERY APPROXIMATE!
    ydist = abs(a[0] - b[0]) * milePerDegree
    xdist = abs(a[1] - b[1]) * cos(abs(a[0]/180.0)) * milePerDegree
    dist = sqrt(xdist ** 2 + ydist ** 2)
    return dist

def swappedLatLong(a, b):
    '''Check for swapped lat/long coordinate pairs '''
    epsilon = .001
    if abs(a[0] - b[1]) < epsilon and abs(a[1] - b[0]) < epsilon:
        return True
    return False

def acre2sqkm(acre):
    return float(acre) * 0.004047 

## TODO split into geo and fbgeo ??

def parseGeocode(geocode):
    '''Parse a Freebase Geocode object into a lat, long [, elev] tuple'''

    lat = geocode['latitude']
    long = geocode['longitude']
    elev = geocode['elevation']

    # It's possible to have objects with no valid contents
    # Make sure we've got a complete lat/long pair
    if lat == None or long == None:
        return None

    # Elevation is optional
    if elev == None:
        return [float(lat), float(long)]
    else:
        return [float(lat), float(long), float(elev)] 


def queryUsStateGuids(session):
    '''Query server and return dict of ids for states keyed by 2 letter state code '''
    query = [{'guid' : None,
               'id' : None,
               'name' : None,
               'iso_3166_2_code' : None,
               'iso:iso_3166_2_code~=' : '^US-*',
               'type' : '/location/administrative_division'
    }]
    results = session.fbRead(query)
    return dict([ (state['iso_3166_2_code'][3:5],state['guid']) for state in results])

def queryCityTownGuid(session, townName, stateGuid):
    '''Query server for town by name and return single Id or None '''
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
            log.warn('Multiple matches for city/town ', townName, ' in state ', stateGuid)
            return None
        log.warn('Multiple matches for city/town ' + townName + ' in state ' + stateGuid +' picked nonCDP ' + result)
        return result
    return None
    
def queryCityTown(session, townName, stateGuid):
    '''Query server and return list of ids for any matching towns '''
    query = [{'guid' : None,
               'id' : None,
               'name' : townName,
               '/location/location/containedby' : [{'guid' : stateGuid}],
               't:type' : '/location/citytown',
               'type' : []
    # TODO - Add a not Census Designated Place term?
               }]
    return session.fbRead(query)

def queryCountyGuid(session, name, stateGuid):
    '''Query server and return ID for matching county in state'''
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
    '''Query server for locations associated with our topic'''
    query = {'guid' :guid,
               'id' : None,
               'name' : None,
               'contained_by' : [],
               'type' : '/location/location'
               }
    results = session.fbRead(query)
    if results != None:
        return results['contained_by']
    return None

def queryGeoLocation(session, guid):
    '''Query server and return Geocode object for given location (or None)'''
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
               'latitude' : None,
               'longitude' : None,
               'elevation' : None,
               'type' : '/location/geocode'
               }
    return session.fbRead(query)  
    
def addGeocode(session, topicGuid, coords):
    query = {'guid': topicId, 
             'type': '/location/location', 
             'geolocation': {'create': 'unless_connected', 
                             'type': '/location/geocode', 
                             'latitude': coords[0], 
                             'longitude': coords[1]
                             }
             }
    if len(coords) > 2:
        query['elevation'] = coords[2]
    return session.fbWrite(query)


def updateGeocode(session, geocodeGuid, coords):
    '''Change the coordinates of an existing geocode'''
    query = {'guid': geocodeGuid, 
             'type': '/location/geocode', 
             'latitude': {'connect' : 'update', 'value' : coords[0]}, 
             'longitude': {'connect' : 'update', 'value' : coords[1]}
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
                