'''
Check for mismatches between GNIS/Geonames name and the Freebase topic name.

Created on Jan 23, 2010

@author: tfmorris
'''

import csv
from fileinput import FileInput
import urllib
import zipfile

from FreebaseSession import FreebaseSession, getSessionFromArgs

baseurl = 'http://geonames.usgs.gov/docs/stategaz/'
filename = 'POP_PLACES_20091002.zip'
workdir = ''

def loadGnis():
    zfile = zipfile.ZipFile(filename, mode='r')
    file = zfile.open(zfile.infolist()[0])
    reader = csv.reader(file,delimiter='|')
    header = reader.next() # get rid of header row
    
    places = {}
    for r in reader:
        places[r[0]] = r[1]
    
    file.close()
    zfile.close()
    
    return places 

def normalizeName(name):
    return name.replace('St.','Saint').replace('Ste.','Sainte').replace("'","").lower().replace('(historical)','').strip()

def main():
#    url = baseUrl + filename
#    urllib.urlretrieve(url, workDir + filename)
    places = loadGnis()

    session = getSessionFromArgs()
    
    q = {"t:type":"/location/citytown",
          "type":"/location/location",
          "gnis_feature_id" : [],
          "name":None,
          "id":None,
          }

    count = 0
    mismatches = 0    
    for r in session.mqlreaditer([q]): 
        count += 1
        name = r['name']
        if name != name.strip():
            print 'Name "%s" has leading/trailing whitespace - %s' % (name,r['id'])
        name = normalizeName(name)
        ids = r['gnis_feature_id']
        if len(ids) > 1:
            print 'Multiple GNIS feature IDs for id %s' % r['id']
        for id in ids:
            i = str(int(id))
            if i != id:
                print 'ID %s not integer - %s' % (id,r['id'])
            if not i in places:
                print 'No GNIS entry for id # %s %s %s' % (i,r['id'],r['name'])
            elif places[i].lower().replace('(historical)','').replace("'","").strip() != name:
                mismatches += 1
                print '%d/%d Name mismatch -\t%s\t%s\t%s' % (mismatches, count, r['id'],r['name'],places[i])


if __name__ == '__main__':
    main()