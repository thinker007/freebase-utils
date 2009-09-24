'''
Read the U.S. National Register of Historic Places database and load it 
into Freebase.

@requires: dbf from http://dbfpy.sourceforge.net/  
@requires: freebase-api from http://freebase-api.code.google.com

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
'''

from __future__ import with_statement

import datetime
import logging
import logging.handlers
import os
import re
import shutil
import tempfile
import time
import urllib
import urllib2
import zipfile

from math import cos, sqrt

from dbfpy import dbf

import fbgeo
from FreebaseSession import FreebaseSession
from names import isNameSeries, normalizeName, normalizePersonName
import NRISkml
from simplestats import Stats
import wikipedia


### Initialization of globals (ick!)
# TODO configuration parameters should all be command line parameters
freebaseHost = 'www.sandbox-freebase.com' # 'www.sandbox-freebase.com' for testing, 'www.freebase.com' for production work
username = 'tfmorris' # set to a valid account for writing
password = 'password'

fetch = True # Fetch files from National Park Service (only published a few times a year)

# Inclusion criteria for states, significance, and resource type
# Empty list [] matches everything
incState = []
incSignificance = ['IN','NA']#,'ST']# ,'LO','NO'] # INternational, NAtional, STate, LOcal, NOt indicated
incResourceType = ['B','S','U','O','D'] # Building, Site, strUcture, Object, District,
incRestricted = True # Include sites with restricted locations (usually archaelogical sites) 
incNonNominated = True # include sites without special designation like National Historical Landmark

createTopics = False # Create new Freebase topics for listings which can't be reconciled

# Use the following parameter to restart a run in the middle if it was interrupted
startingRecordNumber = 0

logging.basicConfig(level=logging.DEBUG)
logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
log = logging.getLogger('NRISupload')
log.setLevel(logging.DEBUG)
   
baseUrl = 'http://www.nr.nps.gov/NRISDATA/'
workDir = ''
masterFile = 'master.exe'
detailFile = 'DETAIL.EXE'
filenames = ['README.TXT', 'SCHEMA.DBF', masterFile, detailFile]
geoBaseUrl = 'http://www.nr.nps.gov/NRISGEO/'
geoFile = 'spatial.mdb'
geoUrl = geoBaseUrl + geoFile
kmzBaseUrl = geoBaseUrl + 'Google_Earth_layers/'
kmzFiles = ['NRHP - Midwest Region.kmz',
            'NRHP - Northeast Region.kmz',
            'NRHP - South Region.kmz',
            'NRHP - Territories Region.kmz',
            'NRHP - West Region.kmz'
            ]
lookupTables = ['CERTM',    # Certification Status LI = Listed, etc
                'STATEM',   # State name
                'COUNTYM',  # County name
                'COUNTYD',  # Place detail (county, city, state)
                'NOMNAMED', # Nominator category
                'LEVSGD',  # Level of significance
                'ARCHTECD', # Architect
                'ARSTYLM',
                'ARSTYLD',
#                'AREASGD', # Area of Significance -> AREASGM.AREASSG (40 categories)
#                'AREASGM',
#                'PERIODD',  # Period (multiple) PERIODCD->PERIODM.PERIOD
#                'PERIODM',
                'SIGYEARD', # Significant year CIRCA = ''|C, SIGYEAR=year (can be multiple years)
                'SIGNAMED', # Significant names
                'CULTAFFD', # Cultural affiliation CULTAFFL
                'OTHNAMED', # Aliases / alternate names
#                'MATD', # Material made of ->MATM.MAT
#                'MATM',
#                'APRCRITD', # Applicable criteria A= Event, B= Person, C= Architecture/Engineering, D=Information Potential
#                'OTHCERTD', # Other certification and date
#                'OTHDOCD', # Sources of other documentation in cultural resource surveys - OTHDOCCD -> OTHDOCM.OTHDOC
#                'OTHDOCM',
                ] #, 'OSTATED']
# lookupTables = ['COUNTYD'] # short list for quicker debugging runs


wikiRE = [re.compile('.ational\s*.egister\s*.f\s*.istoric\s*.laces'),
          re.compile('.ational\s*.onument'),
          re.compile('.ational\s*.emorial')]

stats = Stats()
tables = dict() # Global set of database tables we've read

#### Methods ####
def indexNames(db):
    '''Return a dict indexed by name with a list of refNums for each name'''
    names = dict()
    for rec in db:
        name = normalizeName(rec['RESNAME'])
        refNum = rec['refNum']
        try:
            names[name].append(refNum)
        except KeyError:
            names[name] = [refNum]
            
    nonUniques = [len(names[name]) for name in names if len(names[name]) > 1]
    log.info('Total non-unique names:' + str(len(nonUniques)))
    log.debug(['Counts of non-uniques:', nonUniques])
    return names


# TODO move to someplace general
def queryName(session, name):
    # Query Freebase by name (including aliases) to see if we've got this already
    query = [{'/type/reflect/any_value' : [{'lang' : '/lang/en',
                                            'link|=' : ['/type/object/name',
                                                        '/common/topic/alias'],
                                            'type' : '/type/text',
                                            'value' : name}],
              'name' : None,
              'guid' : None,
              'id' : None,
              'type' : [],
              't:type' : '/common/topic',
              # Exclude NRIS listings because they would have matched exactly on refnum
              't2:type' : {'id':'/base/usnris/nris_listing',
                           'optional':'forbidden'},
              'key' : [{'namespace' : '/wikipedia/en_id',
                       'value' : None
                       }]
              }]
    results = session.mqlread(query)
    return results

def queryTypeAndName(session, type, names, createMissing = False):
    '''Query server for given type by name(s) in name or alias field.  Return empty list if not unique or not found'''
    if not names:
        return []
    
    ids = []
    for name in names:
        query = [{'/type/reflect/any_value' : [{'lang' : '/lang/en',
                                                'link|=' : ['/type/object/name',
                                                            '/common/topic/alias'],
                                                'type' : '/type/text',
                                                'value' : name}],
                  'guid' : None,
                  'type' : type
        }]
        results = session.fbRead(query)
        if not results:
#            log.debug([ 'Warning: name not found ', name, ' type: ', type])
            if createMissing:
                guid = createTopic(session, name, [type])
                if guid:
                    ids.append(guid)
                    log.info('Created new topic ' + str(guid) + '  ' + name)
                else:
                    log.error('Failed to create new entry ' + name + ' type: ' + type)
        elif len(results) == 1:
            ids.append(results[0]['guid'])
#            log.debug(['                                                          found ', name])
        else:
            log.warn('Non-unique name found for unique lookup ' + name +' type: ' + type) 
            # TODO We could create a new entry here to be manually disambiguated later
#            if createMissing:
#                guid = createTopic(session, name, type)
#                if guid:
#                    ids.append(guid)
#                    log.info('Created new topic which may need manual disambiguation ', guid, '  ', name)
#                else:
#                    log.error('Failed to create new entry ', name, ' type: ', type)
    return ids

def queryArchitect(session, names):
    '''Query server for architect by name.  List of GUIDs returned.  Non-unique and not found names skipped.'''
    normalizedNames = [normalizePersonName(n) for n in names]
    # TODO Create missing architects?
    for n in normalizedNames:
        stats.incr('Arch:',str(n))
    return queryTypeAndName(session, '/architecture/architect', normalizedNames)

def remap(key, dict):
    '''Remap a name using the values in dict.  Return original name if not found. '''
    try:
        return dict[key]
    except:
        return key

def uniquifyYears(seq):
    result = []
    for item in seq:
        yr = item[1]
        if int(yr) > 2020:
            yr = yr + " B.C.E"
        if yr and yr not in result:
            result.append(yr)
    result.sort()
    return result

def queryArchStyle(session, codes):
    '''Query server for architectural style by name.  Return None if not unique or not found'''
    # TODO Add skip list (other, multiple, etc)
    for c in '01','80','90': # Remove None, Other, Mixed
        if c in codes:
            codes.remove(c)

    names = [lookup('ARSTYLM', c)[0].lower() for c in codes]
    styleMap = {'bungalow/craftsman' : 'american craftsman',
                'mission/spanish revival' : 'mission revival',
                'colonial' : 'american colonial',
                'pueblo' : 'pueblo revival',
                'chicago' : 'chicago school',
                'late victorian' : 'victorian',
                'modern movement' : 'modern',
                 }
    names = [remap(n,styleMap) for n in names]
    ids = queryTypeAndName(session, '/architecture/architectural_style', names)
    for n in names:
        stats.incr('ArchStyle:',str(n))
    if len(codes) != len(ids):
        log.warn('Failed to find Architecture style name/id(s)' + repr(codes) + repr(names) + ' IDs: ' + repr(ids))
    return ids

def queryNhrpSignificanceIds(session):
    '''Query server and return dict of ids for significance levels keyed by first two letters of name'''
    query = [{'type' : '/base/usnris/significance_level',
              'name' : None,
              'id' : None,
              'guid' : None}]
    results = session.fbRead(query)
    if len(results) < 4:
        log.critical('Expected at least 4 significance levels, got ' + str(results))
    return dict([(item['name'].lower()[0:2], item['guid']) for item in results])

def queryNhrpCategoryGuids(session):
    '''Query server and return dict of ids for categories keyed by name'''
    catProp = '/protected_sites/natural_or_cultural_site_designation/categories'
    query = [{catProp : [
          {
            'guid' : None,
            'id' : None,
            'name' : None
          }],
        'id' : '/en/national_register_of_historic_places'
      }]
    results = session.fbRead(query)
    return dict([(cat['name'].lower(), cat['guid']) for cat in results[0][catProp]])

def addType(session, guids, types):
    for guid in guids:
        query = {'guid': guid, 
                 'type': [{'connect':'insert', 'id':t} for t in types]
                 }
        log.debug('Adding types ' + repr(query))
        response =session.fbWrite(query)

def addAliases(session, guid, aliases):
    if aliases and guid:
        # TODO Filter out low quality aliases
        query = {'guid': guid, 
                 '/common/topic/alias': [{'connect':'insert', 
                                          'lang': '/lang/en',
                                          'type': '/type/text',
                                          'value':a} for a in aliases]
                 }
        log.debug('Adding aliases ' + repr(aliases) + ' to ' + guid)
        return session.fbWrite(query)
        
def addNrisListing(session, guids, listing):
    for guid in guids:
        query = {'guid': guid, 
                 'type': {'connect':'insert', 'id':'/base/usnris/significant_person'},
                 '/base/usnris/significant_person/nris_listing' : {'connect':'insert', 'guid':listing},
                 }
#        log.debug('----Adding listing ' + repr(query))
        response =session.fbWrite(query)


def checkAddGeocode(session, topicGuid, coords):
    '''
    Add geocode to topic if needed.  It will warn if lat/lon appear swapped,
    but *not* fix it.  It also doesn't update the geocode if it's within
    an epsilon (currently 0.1 nm) of the current location.
    '''
    geocode = fbgeo.queryGeoLocation(session, topicGuid)
    if not geocode:
        response = fbgeo.addGeocode(session, topicGuid, coords)
    else:
        response = None
        location = fbgeo.parseGeocode(geocode)
        if location:
            if fbgeo.swappedLatLong(location, coords):
                print '*** Lat/long appear swapped ', geocode, coords
            #   print '*** Swapping geocode lat/long', geocode, coords
            #   response = fbgeo.updateGeocode(session, geocode['guid'], coords[:2])
            else:
                distance = fbgeo.approximateDistance(location, coords)
                if (distance > 0.1):
                    log.debug('Skipping topic with existing geo info ' + str(topicGuid) + ' distance = ' + str(distance) + ' Coords = ' + repr(coords) + ' ' + repr(location))
    return response


def addListedSite(session, topicGuid, categoryGuid, certDate):
    # TODO check for a listed date which is just a year and update it
    query = {'guid': topicGuid, 
             'type': '/protected_sites/listed_site', 
             'designation_as_natural_or_cultural_site': 
                {'create': 'unless_connected', 
                 'type': '/protected_sites/natural_or_cultural_site_listing', 
                 'designation': {'connect': 'insert', 
                                 'id': '/en/national_register_of_historic_places'
                                 }, 
                  'date_listed': certDate.isoformat()
                  }
              }
    if categoryGuid:
        query['designation_as_natural_or_cultural_site']['category_or_criteria'] = {'connect': 'insert', 
                                                                                    'guid': categoryGuid}    
    return session.fbWrite(query)

def significance2guid(significance, significanceGuids):
    if significance:
        s = significance.lower()
        if s in significanceGuids:
            return significanceGuids[s]

def updateTypeAndRefNum(session, topicGuid, refNum, resourceType, mandatoryTypes, significanceGuid):
    types = ['/location/location',
             '/protected_sites/listed_site', 
             '/base/usnris/topic', 
             '/base/usnris/nris_listing',
             ]
    if resourceType == 'B':
        types.append('/architecture/building')
        types.append('/architecture/structure')
        types.append('/location/location')
    elif resourceType == 'S':
        pass
    elif resourceType == 'D':
        pass
    elif resourceType == 'U':
        pass
    # Some of these are boats, so we can't use structure
    #            types.append('/architecture/structure')
    # TODO What types?
    elif resourceType == 'O':
        # TODO: What types of for objects?
        pass
    else:
        log.error('unknown resource type ' + resourceType + ' for topic ' + topicGuid)
        # If we've got an area to record, add the Location type no matter what
    for t in mandatoryTypes:
        if not t in types:
            types.append(t)
    # Add any missing types, our unique reference number, & significance level
    query = {'guid': topicGuid, 'type': [{'connect': 'insert', 'id': t} for t in types], '/base/usnris/nris_listing/item_number': {'connect': 'insert', 'value': refNum}}
    if significanceGuid:
        query['/base/usnris/nris_listing/significance_level'] = {'connect': 'update', 
                                                                 'guid': significanceGuid}
    stats.incr('General', 'TopicsUpdated')
    log.debug('  Writing guid:' + topicGuid)
    return session.fbWrite(query)


def addBuildingInfo(session, streetAddress, topicGuid, stateGuid, cityTownGuid, 
                    archIds, archStyleIds):
    query = {'guid': topicGuid, 
             'type': '/architecture/structure'}
    # TODO refactor into fbGEO
    addressSubQuery = {}
    if streetAddress:
        addressSubQuery.update({'street_address': streetAddress, 
                                'state_province_region': {'connect': 'insert', 
                                                          'guid': stateGuid}})
    if cityTownGuid:
        addressSubQuery.update({'citytown' : {'connect': 'insert', 
                                        'guid': cityTownGuid}})
    if addressSubQuery:
        addressSubQuery.update({'create': 'unless_connected', 
                                'type': '/location/mailing_address'})
        query['address'] = addressSubQuery
        
    if archIds:
        query['architect'] = [{'connect': 'insert', 'guid': i} for i in archIds]
        stats.incr('Wrote', 'Architect')
    if archStyleIds:
        query['architectural_style'] = [{'connect': 'insert', 'guid': i} for i in archStyleIds]
        stats.incr('Wrote', 'ArchStyle')    
    return session.fbWrite(query)


def addMisc(session, topicGuid, significantPersonIds, significantYears, culture):
    query = {}
    if significantYears:
        query['/base/usnris/nris_listing/significant_year'] = [{'connect': 'insert', 'value': y} for y in significantYears]
    
    if significantPersonIds:
        addNrisListing(session, significantPersonIds, topicGuid)
        query['/base/usnris/nris_listing/significant_person'] = [{'connect': 'insert', 'guid': guid} for guid in significantPersonIds]
    # TODO add /base/usnris/significant_person type to person objects
    
    if culture:
        query['/base/usnris/nris_listing/cultural_affiliation'] = [{'connect': 'insert', 'lang': '/lang/en', 'value': c} for c in culture]
    # TODO: Try to match free form text to a /people/ethnicity topic
    
    if query:
        query['guid'] = topicGuid
        return session.fbWrite(query)
           
def createTopic(session, name, types):
    common = "/common/topic"
    if not common in types: # make sure it's a topic too
        types.append(common)
    query = {'create': 'unconditional', # make sure you're checked to be sure it's not a duplicate
             'type': types,
             'name' : name,
             'guid' : None
             }
    response = session.fbWrite(query)
    guid = response['guid']
    return guid

def createPerson(session, name):
    # Might not be a good idea to assume they're all deceased, but it's a *historic* database
    return createTopic(session, name, ["/people/person","/people/deceased_person"])

def queryNrisTopic(session, refNum):
    '''Query server for a unique topic indexed by our NRIS reference number'''
    query = [{'guid' : None,
               'id' : None,
               'name' : None,
               '/base/usnris/nris_listing/item_number' : refNum,
               'type' : '/protected_sites/listed_site'
               }]
    results = session.fbRead(query)
    if results:
        if len(results) == 1:
            return results[0]['guid'],results[0]['name']
        elif len(results) > 1:
            log.error('multiple topics with the same NHRIS reference number ' + refNum) 
    return None, None

def queryTopic(session, refNum, name, aliases, exactOnly):
    # Look up by Ref # enumeration first
    topicGuid,topicName = queryNrisTopic(session, refNum)

    if not topicGuid:
        results = queryName(session, name)
        incrMatchStats(results)
        
        wpids = extractWpids(results)
        wpid = wikipedia.queryArticle(wpids, refNum, wikiRE, exactOnly)
        item = wpid2Item(results, wpid)
#            if result == None:
#                log.debug( 'no Wikipedia match found ' + refNum + ' ' + repr(name))
#            else:
#                log.debug( '  Found ' + quality + ' match ' + result['guid'] + ' ' + repr(result['name']) )
                    
        if not item and aliases:
            log.debug('Trying aliases' + str(aliases))
            for n in aliases:
                results = queryName(session, n)
                wpids = extractWpids(results)
                wpid = wikipedia.queryArticle(wpids, refNum, wikiRE, exactOnly)
                item = wpid2Item(results, wpid)
                if item:
                    log.info('**Resolved using alias ' + n + ' for name ' + name)
                    break
#            if item == None:
#                log.debug('Trying reconciliation service')
#                result = reconcileName(session, name, ['/protected_sites/listed_site'])
#                log.debug('Reconciliation service return result = ' + str(result))
#                if result['recommendation'] == 'automerge':
#                    # TODO need to get GUID
#                    item = result['results'][0]
#                    log.debug('Reconciliation succeeded, recommended = ' + str(result['result'][0]))
#
#                # TODO implement reconciliation service
#                    stats.incr('TopicMatch','ReconciliationServiceMatch')
            
        if item:
           topicGuid = item['guid']
           topicName = item['name']
    else:
        stats.incr('TopicMatch','RefNumEnumerationMatch')
    return topicGuid,topicName
            
def loadTable(dbFile):
    '''Load a table with index in the first column into a dict'''
    result = dict()
    log.debug('Loading table ' + str(dbFile))
    db = dbf.Dbf(dbFile, readOnly=1)
    # We assume the the first column is the key and the remainder the
    # value(s).  If there's only one value column, it's stored directly
    # otherwise we create a map keyed by column name (we could probably 
    fields = db.fieldNames
    log.debug('Reading fields. Key= ' + fields[0] + ' values = ' + str(fields[1:]))
    result['__fields__'] = fields
    for rec in db:
        record = rec.asList()
        key = record[0]
        if not result.has_key(key):
            result[key] = []
        if len(record) == 2:
            result[key].append(record[1])
        else:
            result[key].append(record[1:])
    db.close
    return result

def unzipFiles(files, tempDir):
    for f in files :
        log.debug( '==Unzipping ' + f)
        zfile = zipfile.ZipFile(f, mode='r')
        for name in zfile.namelist():
            bytes = zfile.read(name)
            with open(tempDir + name, 'w') as f:
                f.write(bytes)
        zfile.close()
    log.debug('Unzip complete')


def reconcileName(session, name, types):
    try:
        response = session.reconcile(name, types)
    except:
        log.error('**Freebase reconciliation service failed : ' + name + repr(types))
        return []  
    return response


def lookup1(table, key):
    result = lookup(table, key)
    if result == None or result == '':
        return result
    if len(result) > 1:
        return None
    return result[0]

def lookup(table, key):
    try:
        t = tables[table]
        try:
            return t[key]
        except KeyError:
            return ''
    except KeyError:
        log.critical('Unknown table: ' + table)
        return ''

def lookupAliases(refNum):
    alias = lookup('OTHNAMED', refNum)
    aliases = []
    if alias:
        for a in alias:
            aliases.extend(a.split(';'))
        for a in aliases:
            if a.lower().find('see also') >= 0: # See alsos aren't real aliases
                aliases.remove(a)
    return aliases
    
def incrMatchStats(results):
    numResults = len(results)
    if (numResults) == 0:
        stats.incr('TopicMatch','Match0')
#        log.debug(['No match Freebase name match        ', name])
    elif (numResults > 1) :
        stats.incr('TopicMatch','MatchN')
#        log.debug(['Multiple (', numResults,') Freebase name/alias matches      ', name
    else:
        stats.incr('TopicMatch','Match1')
#       log.debug([ '                 ', name

    # Check if already a Listed Site
    for result in results:
        if '/protected_sites/listed_site' in result['type']:
            stats.incr('ListedSite','True')
        else:
            stats.incr('ListedSite','False')
    return

def extractWpids(items):
    wpids = []
    for item in items:
        for key in item['key']:
            # We don't need to check namespace because it's constrained by the query
            wpids.append(key['value'])
    return wpids
            
def wpid2Item(items,wpid):
    item = None
    if wpid and items:
        for i in items:
            for key in i['key']:
                if wpid == key['value']:
                    item = i
    return item


def acre2sqkm(acre):
    if acre == '9': # Special signal value indicating < 1 acre
        acre = 0    
    if acre == '':
        acre = 0
    # TODO double check to be sure this is 1/10s of an acre
    return fbgeo.acre2sqkm(float(acre) * 0.1)


            
def main():
    
    log.info(''.join(['Selection criteria : States = ', str(incState),
                       ', Significance = ',str(incSignificance),
                       ', Types = ',str(incResourceType)]))
    log.info('Create topics = ' + str(createTopics))
    log.info('Starting record number = ' + str(startingRecordNumber))
    startTime = datetime.datetime.now()
    log.info('Starting on ' + freebaseHost + ' at ' + startTime.isoformat())
    
    # Make temporary directory
    tempDir = tempfile.mkdtemp(suffix='dir', prefix='tempNRISdata') + '/'

    #fetch = isFetchNeeded()
    if fetch :
        for filename in filenames:
            url = baseUrl + filename
            log.info('Fetching ' + url)
            urllib.urlretrieve(url, workDir + filename)
        for filename in kmzFiles:
            url = kmzBaseUrl + urllib.quote(filename)
            log.info('Fetching ' + url)
            urllib.urlretrieve(url, workDir + filename)
#        log.info('Fetching ' + geoUrl)
#        urllib.urlretrieve(geoUrl, workDir + geoFile)
    else:
        log.debug('Using local files (no fetch from NHRIS web site)')
        
    # Unzip our two compressed files into a temporary directory
    unzipFiles([workDir+masterFile, workDir+detailFile], tempDir)         
    
    # Load geo data
    log.info('Loading geo data from KML/KMZ files')
    coordinates = {}
    # TODO: Switch this to use spatial.mdb
    coordinates = NRISkml.parseFiles([workDir + f for f in kmzFiles])
    
    # Read in all our master tables '*M.DBF' and detail xref tables *D.DBF
    for table in lookupTables:
        tables[table] = loadTable(tempDir + table + '.DBF')

    # Lookup and cache the column indexes in our location (aka county) table
    countyTable = tables['COUNTYD']
    stateColumn = countyTable['__fields__'].index('STATECD') - 1
    cityColumn = countyTable['__fields__'].index('CITY') - 1
    primeColumn = countyTable['__fields__'].index('PRIMEFLG') - 1
    vicinityColumn = countyTable['__fields__'].index('VICINITY') - 1
    countyColumn = countyTable['__fields__'].index('COUNTYCD') - 1

    # Establish session
    session = FreebaseSession(freebaseHost, username, password)
    session.login()

    # Query server for IDs of states, categories, and significance levels
    catGuids = queryNhrpCategoryGuids(session)
    significanceGuids = queryNhrpSignificanceIds(session)
    # TODO: We could lookup and cache IDs for Architects, and Architectural Styles too
    
    db = dbf.Dbf(tempDir + 'PROPMAIN.DBF')
    log.debug('Main property fields ' + str(db.fieldNames))

    log.info('** Pre-scanning for duplicate names **')
    names = indexNames(db)

    totalCount = len(db)
    log.info('** Processing ' + str(totalCount) + ' records in main record table **')
    count = 0
    try:
        for rec in db:
            # TODO do we want a try block here to allow us to continue with other records
            # if we have a problem with this one
            count += 1
            stats.incr('General','TotalRecords')
            if count < startingRecordNumber:
                continue
            # Only entries which are Listed or National Landmarks count for our purposes
            status = rec['CERTCD']
    #        stats.incr('CertStatus',status)
            if not status == 'LI' and not status == 'NL':
                continue
    
            refNum = rec['REFNUM'] # key which links everything together
    
            stats.incr('CertStatus','ListedRecords')
            d = rec['CERTDATE']
            certDate = datetime.date(int(d[0:4]),int(d[4:6]),int(d[6:8]))
    
            # Significance Level IN=International, NA=National, ST=State, LO=Local, NO=NotIndicated
            significance = lookup1('LEVSGD', refNum);
            stats.incr('Significance',str(significance))
    
            # Building, District, Object, Site, strUcture
            # U used for some non-structure type things like boats, etc, so be careful!
            resourceType = rec['RETYPECD']
            stats.incr('ResourceType', str(resourceType))
    
            # Skip records which don't match our significance or resource type criteria
            if len(incSignificance) > 0 and not significance in incSignificance:
                continue
            if len(incResourceType) > 0 and not resourceType in incResourceType:
                continue
    
            name = normalizeName(rec['RESNAME'])
                        
            restricted = (not rec['RESTRICT'] == '') # address restricted
            if restricted:
                stats.incr('General','LocationInfoRestricted')
                # Skip restricted address sites for now (mostly archaelogical sites)
                if not incRestricted:
    #               log.debug([ 'Skipping restricted site location', restricted,refNum, name])
                    continue
                        
            streetAddress = rec['ADDRESS']
            area = acre2sqkm(rec['ACRE'])
            
            if not refNum in countyTable:
                log.warn('Warning - no county for ' + ' '.join([refNum, restricted, name]))
                state=''
                cityTown=''
            else:
                for entry in countyTable[refNum]:
                    if entry[primeColumn] != '':
                        state = entry[stateColumn]
                        county = lookup1('COUNTYM',entry[countyColumn])[0]
                        if entry[vicinityColumn] == '':
                            cityTown = entry[cityColumn]
                        else:
                            # Just in the vicinity of, don't record contained by
                            cityTown = ''
           
            category = lookup('NOMNAMED', refNum)
            categoryGuid = None
            if category:
                category = category[0].lower().strip()
                if category in catGuids:
                    categoryGuid = catGuids[category]
                
            # Skip if not a National Historic Landmark, etc
            if not incNonNominated and category == '':
                continue
            
            # Skip states not selected
            stats.incr('State', str(state))
            if len(incState) > 0 and not state in incState:
                continue 
    
            aliases = lookupAliases(refNum)
            # We used to only require an exact match if we had more than one listing
            # with a name, but we're being stricter now to prevent potential false matches
            #topicGuid,topicName = queryTopic(session, refNum, name, aliases, len(names[name]) > 1)
            topicGuid,topicName = queryTopic(session, refNum, name, aliases, True)
            
            # TODO return a list of candidate topics to be queue for human review
    
            # TODO Check for incompatible/unlikely types (book, movie, etc)
            # TODO Check for compatible/reinforcing types (e.g. anything from protected site)
            # TODO Check Building.Address and Location.ContainedBy for (in)compatible addresses
            
            # Still don't have a match, punt...
            if not topicGuid:
                if createTopics:
                    # TODO queue potential new topics for human verification?
                    topicGuid = createTopic(session, name, [])
                    topicName = name
                    log.debug('No Freebase topic - created ' 
                              + ' '.join([topicGuid, refNum, resourceType, state, str(significance), name, ' - ', category]))
                    stats.incr('TopicMatch','CreatedNew')
                else:
                    log.debug('No Freebase topic found - skipping - ' 
                              + ' '.join([refNum, resourceType, state, str(significance), name, ' - ', category]))
                    stats.incr('TopicMatch','NotFound')
                    continue
                    
            
            aliases.append(name)
            if topicName in aliases:
                aliases.remove(topicName) # We might have matched on an alias
            addAliases(session, topicGuid, aliases)
            
            # 'FM' Federated States of Micronesia is in database, but not a real state
            cityTownGuid = None
            containedByGuid = None
            stateGuid = fbgeo.queryUsStateGuid(session, state)
    
            if stateGuid:
                if cityTown:
                    cityTownGuid = fbgeo.queryCityTownGuid(session, cityTown, stateGuid, county)
                    if not cityTownGuid:
                        # TODO One cause of this are city/town pairs with the same name
                        # they often can be treated as a single place, so we might be able to figure
                        # out a way to deal with this
                        log.warn('Failed to look up city/town '+cityTown+' in '+county+', '+state)
                    containedByGuid = cityTownGuid
                # Use county as backup if vicinity flag was set or our town lookup failed
                if not cityTownGuid:
                    containedByGuid = fbgeo.queryCountyGuid(session, county, stateGuid)
            
            # TODO definition of this field is actually "architect, builder, or engineer"
            # so we could try harder to find a match in other disciplines
            # currently we throw away any builders or engineers
            archIds = queryArchitect(session, lookup('ARCHTECD', refNum))
            archStyleIds = queryArchStyle(session, lookup('ARSTYLD', refNum))

            # TODO Do this later when we have a human review queue set up
#            significantNames = [normalizePersonName(n) for n in lookup('SIGNAMED', refNum)]
#            significantPersonIds = queryTypeAndName(session, '/people/person', significantNames, True)            
#            significantPersonIds = queryTypeAndName(session, '/people/person', significantNames, False)
            significantPersonIds = []

            significantYears = uniquifyYears(lookup('SIGYEARD', refNum))
    
            culture = lookup('CULTAFFD', refNum)
            if culture:
                for c in culture:
                    stats.incr('Culture', c)
                log.debug(' Culture: ' + str(culture))
            
            log.debug( '  %2.0f%% ' % (count*100.0/totalCount) + ' '.join([str(count), refNum, resourceType, state, str(significance), str(certDate), name, category]))
    
            # Write/update information
            sigGuid = significance2guid(significance, significanceGuids)
            mandatoryTypes = ['/location/location'] if area > 0 else []
            response = updateTypeAndRefNum(session, topicGuid, refNum, resourceType, mandatoryTypes, sigGuid)
    
            # Handle location
            if resourceType == 'B': # TODO add type str'U'cture ?
                query = addBuildingInfo(session, streetAddress, topicGuid, stateGuid, 
                                        cityTownGuid, archIds, archStyleIds)
    
            if stateGuid:
                containerGuids = [stateGuid]
                if containedByGuid and containedByGuid != stateGuid:
                    containerGuids.append(containedByGuid)
                response = fbgeo.addContainedBy(session, topicGuid, containerGuids)

                if area > 0:
                    response = fbgeo.addArea(session, topicGuid, area)
                
                if refNum in coordinates:
                    coords = coordinates[refNum][:2] # ignore elevation, it's always 0
                    response = checkAddGeocode(session, topicGuid, coords)
    
            # Add Listed Site info
            # TODO: Check for existing entry that we can update with more specific date, category, etc
            response = addListedSite(session, topicGuid, categoryGuid, certDate)
    
            # Add any significance year, people and cultural affiliations
            addMisc(session, topicGuid, significantPersonIds, significantYears, culture)
            
            # TODO: Create a 2nd listing if we've got two certification dates and statuses
            
    #        if name.lower().find(' and ') >= 0:
    #            stats.incr('PossibleCompoundTopic')
                # Flag/log somewhere

    finally:
        db.close()
        endTime = datetime.datetime.now()
        log.info('Ending at ' + str(endTime) + '  elapsed time = ' + str(endTime-startTime))
        log.info('==Statistics==')
        log.info(stats.dump())
    
    # Clean up our temporary directory
#    print 'Cleaning ', tempDir
#    shutil.rmtree(tempDir, True)
    
if __name__ == '__main__':
     main() 





