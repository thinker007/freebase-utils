'''
Created on Mar 6, 2009

@author: Tom Morris <tfmorris@gmail.com>
'''

from __future__ import with_statement

import codecs
import csv
from datetime import datetime

from freebase.api import HTTPMetawebSession, MetawebError

from freebase_person import FbPerson

host = 'www.freebase.com'
username = 'tfmorris'
password = 'tommorris'

desired_types = ['/people/person', 
                '/people/deceased_person', 
                '/government/politician',
                '/user/tfmorris/default_domain/us_congressperson'
                ]

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def name_parse(person, name):
    # TODO handle parentheses
    pieces = [n.strip() for n in name.split(',')]

    person._family_name = pieces[0].lower().title()

    p = pieces[1].find('(')
    if p >= 0:
        q = pieces[1].find(')', p)
        if q >=0:
            person._nickname = pieces[1][p + 1 : q]
            pieces[1] = pieces[1][:p].strip() + ' ' + pieces[1][q + 1:].strip()
            pieces[1] = pieces[1].strip()
 
    person._given_names = pieces[1].split()

    if len(pieces) > 2:
        person._suffix = pieces[2]

def write_thomas_id(session, person, thomas_id):
    types = ['/base/uspolitician/u_s_congressperson',
             #'/base/uspolitician/topic', # For Freebase's kludgy base system
            ]#,'/government/politician']
    query = {'id' : person._id,
             'type' : [{'connect' : 'insert', 'id' : t} for t in types],
             '/base/uspolitician/u_s_congressperson/thomas_id' 
                : {'connect' : 'insert',
                   'value' : thomas_id},
            }
    try:
        status = session.mqlwrite(query)
        return status
    except MetawebError,e:
        print '** Error on query', e, query
    
def main():
    start_time = datetime.now()
    start_count = 375 # 11243 + 139 + 238  # 10094 + 150 + 514
    if username:
        session = HTTPMetawebSession(host, username, password)
        session.login()
    else:
        session = HTTPMetawebSession(host)
    unique = 0
    multiple = 0
    zero = 0

    f = codecs.open('bioguide1.csv', 'r','utf-8', 'replace') 
    c = unicode_csv_reader(f)
    header = c.next()
    count = 0
    for r in c:
        count += 1
        if count < start_count:
            continue
        thomas_id = r[0]
        person = FbPerson()
        name_parse(person, r[1])
        person._birth_date = r[2]
        if r[3] != '':
            person._death_date = r[3]

#        found = person.resolve(session, desired_types)
        found = person.resolve(session,['/government/politician'])
        if found == 1:
            unique += 1
            print unique, zero, multiple, "                       Match", thomas_id, person.format_name_with_dates().encode('utf-8','ignore'), person._id
            result = write_thomas_id(session, person, thomas_id)
        elif found == 0:
            zero += 1
            print unique, zero, multiple, "No match", thomas_id, person.format_name_with_dates().encode('utf-8','ignore'), person._id
        else:
            multiple += 1
            print unique, zero, multiple, '  ', found, "matches", thomas_id, person.format_name_with_dates().encode('utf-8','ignore'), person._id



    f.close()
    total = unique + zero + multiple
    print 'Unique matches: ', unique, '%2.0f%% ' % (unique * 100.0 / total)
    print 'Unable to match: ',  zero, '%2.0f%% ' % (zero * 100.0 / total)
    print 'Multiple matches: ', multiple, '%2.0f%% ' % (multiple * 100.0 / total)
    print 'Total records: ', count
    print 'Elapsed time: ', datetime.now() - start_time



    
if __name__ == '__main__':
    main()