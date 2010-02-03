'''
Module to parse a National Park Service KML file containing locations for the
National Register of Historic places.

Created on Feb 27, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
'''


from datetime import datetime
import logging
import re
from xml.sax import parseString
from xml.sax.handler import ContentHandler
import zipfile

_log = logging.getLogger('fbkml')

kmzFiles = ['NRHP - Midwest Region.kmz',
            'NRHP - Northeast Region.kmz',
            'NRHP - South Region.kmz',
            'NRHP - Territories Region.kmz',
            'NRHP - West Region.kmz'
            ]


class KmlHandler(ContentHandler):
    '''Parse a KML file for the National Park Service National Register of Historic Places'''

    #geocodeRE = re.compile('Geocode Match: </b>([0|1])')
    #refnumRE = re.compile('NPS Reference Number: </b>([0-9]{8})')
    RE = re.compile('.*?(?s)NPS Reference Number:\\s*</b>(\\d{8}).*?(?:Geocode Match: </b>([0-1])).*?',re.DOTALL)
    
    def __init__(self):
        self.level = 0
        self.buffer = ''
        self.count = 0
        self.geocodedCount = 0
        self.points = {}
        self.refNum = ''
        self.name = ''
        self.coordinates = []

    def setDictionary(self, points):
        self.points = points
        
    def startElement(self, name, attrs):
#        print '                      '[:self.level],'Starting ', name, " - ",
        self.level += 1
        self.buffer = ''
        # TODO push element on stack
        if name == 'Placemark':
            self.coordinates = []
            self.refNum = ''
            self.name = ''
            pass
        elif name == 'Point':
            self.coordinates = []
            pass
        return
    
    def characters(self, ch):
        self.buffer += ch
    
    def endElement(self, name):
        self.level -=1
#        print self.buffer
        if name == 'Placemark':
            # TODO check for missing info
            self.points[self.refNum] = self.coordinates
            pass
        elif name == 'description': # Placemark/description
            # <b>NPS Reference Number: </b>88000612<br />
            # <b>Geocode Match: </b>1<br />
            match = self.RE.match(self.buffer)
            if match:
                self.refNum = match.group(1)
                geocode = match.group(2)
                self.count+=1
                if geocode == '1':
                    self.geocodedCount += 1
        elif name == 'name': # Placemark/name
            self.name = self.buffer
            self.buffer = ''            
        elif name == 'coordinates': # Placemark/Point/coordinates
            # Triple long, lat, elev   -64.9974736069999,18.3551051620001,0
            coords = self.buffer.split(',')
            long = float(coords[0])
            lat = float(coords[1])
            if len(coords) > 2:
                elev = float(coords[2])
                # TODO this is order dependent and assumes that the description
                # element comes before the Point element - true currently, but not guaranteed
                self.coordinates = [long, lat, elev]
            else:
                self.coordinates = [long, lat, None]
#            print " Lat = ", lat, " long = ", long, " elev = ", elev
        self.buffer = ''
        
def parse(file, coordinates):
    handler = KmlHandler()
    handler.setDictionary(coordinates)
    # TODO - test for kmz vs kml
    if False:
        kmlFile = open(file)
        parser = make_parser()
        parser.setContentHandler(handler)  
        parser.parse(kmlFile)
    else:
        kmlFile = zipfile.ZipFile(file, 'r')
        entries = kmlFile.filelist
        parseString(kmlFile.read(entries[0].filename), handler)

    _log.debug("Loaded %d coordinate pairs (%d geocoded)." % (handler.count, handler.geocodedCount))
    return handler.coordinates

def parseFiles(files = kmzFiles):
    coords = {}
    for f in files:
        parse(f, coords) 
    return coords
    
if __name__ == '__main__':
   startTime = datetime.now()
   results = parseFiles()
   print "Loaded %d (%d geocoded) entries in %t" % (len(results), self.geocodedCount,(datetime.now() - startTime))
#   print results