'''
Created on Mar 6, 2009

This data loader works with the GovTrack.us data file people.xml containing
all U.S. Congress people which can be fetched from

  http://www.govtrack.us/data/us/<congress>/repstats/people.xml
  e.g. http://www.govtrack.us/data/us/111/repstats/people.xml

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
'''

from __future__ import with_statement

import codecs
from datetime import datetime
from xml.sax import make_parser
from xml.sax.handler import ContentHandler

from freebase.api import HTTPMetawebSession, MetawebError

from freebase_person import FbPerson

host = 'api.sandbox-freebase.com' # 'api.freebase.com'
username = None #'tfmorris'
#password = 'password'
write = False

desired_types = ['/people/person', 
                '/people/deceased_person', 
                '/government/politician',
                '/user/tfmorris/default_domain/us_congressperson'
                ]


class CongresspersonXmlHandler(ContentHandler):
    '''Parse a GovTrack person.xml file'''
    
    def __init__(self):
        self.level = 0
        self.person_count = 0
        self.session = None
        self.unique = self.zero = self.multiple = self.id_match = 0
        self.current_count = 0

    def setSession(self,session):
        self.session = session
        
    def parse_person(self, attrs):
        p = FbPerson()
#        p._id = attrs['bioguideid'] # Don't fill this in until we check Freebase
        #            p._name = attrs['name'] # preferred name form?
        p._family_name = attrs['lastname']
        p._given_names = [attrs['firstname']]
        if 'middlename' in attrs:
            p._given_names.append(attrs['middlename'])
        
        if 'nickname' in attrs:
            p._nickname = attrs['nickname']
        
        if 'namemod' in attrs:
            p._name_suffix = attrs['namemod']
        
        if 'title' in attrs:
            # Title is just "Rep." or "Sen." which adds no value to search - skip it
#            p._title = attrs['title']
            pass
        
        if 'birthday' in attrs:
            bdate = attrs['birthday']
            if not bdate == '0000-00-00':
                p._birth_date = bdate

        if 'gender' in attrs:
            p._gender = attrs['gender']
        #            p._religion = attrs['religion']
        # No death date/year?
        
        for a in attrs.keys():
            if not a in ['bioguideid', 'gender', 
                         'name', 'lastname', 'firstname', 'middlename', 
                         'nickname', 'title', 'namemod', 'birthday', 
                         'religion', 'osid', 'metavidid', 'youtubeid', 'id', 
                         'state', 'district', # Shouldn't this be part of role?:
                         ]: 
                print 'Unknown person attribute ', a
                    
        return p


    def startElement(self, name, attrs):
#        print '                      '[:self.level],'Starting ', name, " - ", repr(attrs)
        self.level += 1
        # TODO push element on stack
        if name == 'people':
            pass
        elif name == 'person':
            self.person_count += 1
            self.current_listing = None
            self.person = self.parse_person(attrs)
            if 'bioguideid' in attrs:
                self.id = attrs['bioguideid']
            else:
                print 'Skipping entry with no BioGuide ID' + attrs['name']
                # person.xml now includes presidents with no Congressional bioguide ID
                return
            # Resolve person against Freebase
            # write ID and other info to freebase
#            print self.person_count, person._id, person.format_name_with_dates()

        elif name == 'role':
            if attrs['startdate']:
                d = datetime.strptime(attrs['startdate'],'%Y-%m-%d')
                if d > datetime(2008,9,1):
                    self.current_count += 1
                    self.current_listing = ' '.join(["current", \
                     str(self.current_count), attrs.get('type', '??'), \
                        attrs.get('state', '??'), str(attrs.get('district', '??')), 
                        ])
            for a in attrs.keys():
                if not a in ['type', # rep or sen
                             'startdate',
                             'enddate',
                             'party',
                             'state', # 2 letter code
                             'district', # empty for Senate, -1 for pre-district Reps
                             'url',
                             ]:
                    print 'Unknown role attribute ', a
        elif name == 'current-committee-assignment':
            for a in attrs.keys():
                if not a in ['committee',
                             'role',
                             'subcommittee' 
                             ]:
                    print 'Unknown current-committee-assignment attribute ', a            
        else:
            print '** Unknown element', name
        
        return
    
    def characters(self, ch):
#        self.buffer += ch
        pass
    
    def endElement(self, name):
        self.level -=1
        if name == 'person':
            if self.current_listing:
                self.handle_person(self.session, self.person, self.id)
                print self.current_listing, self.id, self.person.format_name_with_dates(), self.person._id
        elif name == 'people':
            total = self.unique + self.zero + self.multiple + self.id_match
            print 'ID matches: ', self.id_match, '%2.0f%% ' % (self.id_match * 100.0 / total)
            print 'Unique matches: ', self.unique, '%2.0f%% ' % (self.unique * 100.0 / total)
            print 'Unable to match: ',  self.zero, '%2.0f%% ' % (self.zero * 100.0 / total)
            print 'Multiple matches: ', self.multiple, '%2.0f%% ' % (self.multiple * 100.0 / total)
            print 'Total records: ', self.person_count


    def handle_person(self, session, person, thomas_id):
        result = self.query_thomas_id(session, thomas_id)
        if result:
            self.id_match += 1
            id = person._id = result['id']
            # TODO add code to verify against XML file
#            print 'Skipping ', id, person.format_name_with_dates()
            return
        
        found = person.resolve(session,['/government/politician'])
        if found == 1:
            self.unique += 1
            print self.person_count, self.unique, self.zero, self.multiple, \
                "                       Match", thomas_id, \
                person.format_name_with_dates().encode('utf-8','ignore'), person._id
#            result = self.write_thomas_id(session, person, thomas_id)
        elif found == 0:
            self.zero += 1
            print self.person_count, self.unique, self.zero, self.multiple, \
                "No match", thomas_id, \
                person.format_name_with_dates().encode('utf-8','ignore'), person._id
        else:
            self.multiple += 1
            print self.person_count, self.unique, self.zero, self.multiple, '  ', found, "matches", \
                thomas_id, person.format_name_with_dates().encode('utf-8','ignore'), person._id

    def query_thomas_id(self, session,thomas_id):
        types = ['/base/uspolitician/u_s_congressperson',
                 #'/base/uspolitician/topic', # For Freebase's kludgy base system
                ]#,'/government/politician']
        query = {'type' : '/base/uspolitician/u_s_congressperson',
                 'thomas_id' : thomas_id,
                 'id' : None,
                 'guid' : None,
                 'name' : None,
                }
        try:
            status = session.mqlread(query)
            return status
        except MetawebError,e:
            print '** Error on query', e, query    
    
    def write_thomas_id(self, session, person, thomas_id):
        if not write:
            return
        
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
    if username:
        session = HTTPMetawebSession(host, username, password)
        session.login()
    else:
        session = HTTPMetawebSession(host)

    handler = CongresspersonXmlHandler()
    handler.setSession(session)
    xmlfile = open('people.xml')
    parser = make_parser()
    parser.setContentHandler(handler)  
    parser.parse(xmlfile)

    xmlfile.close()
    print 'Elapsed time: ', datetime.now() - start_time


if __name__ == '__main__':
    main()
