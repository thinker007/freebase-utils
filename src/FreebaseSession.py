'''
Created on Aug 8, 2009

@author: Tom Morris <tfmorris@gmail.com>
@license: Eclipse Public License
@copyright: 2009 Thomas F. Morris
'''

import json
import logging
from optparse import OptionParser

from freebase.api import HTTPMetawebSession, MetawebError

SEPARATORS = (",", ":")

def getSessionFromArgs():
    '''Convenience method to create a Freebase session using the username, 
    password, and host in the command line arguments.
    Session is NOT logged in on return.'''
    
    log = logging.getLogger('FreebaseSession')
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", help="Freebase username")
    parser.add_option("-p", "--password", dest="pw", help="Freebase password")
    parser.add_option("-s", "--host", dest="host", help="service host", default = 'www.sandbox-freebase.com')
   
    (options, args) = parser.parse_args()

    user = options.user
    pw = options.pw
    host = options.host
    
    # TODO not sure this is a good idea...
#    if not pw:
#        pw = getpass.getpass()

    log.info( 'Host: %s, User: %s' % (host, user))
    return FreebaseSession(host, username=user, password=pw)

class FreebaseSession(HTTPMetawebSession):
    '''
    Extended version of HTTPMetawebSession to allow wrappers with our own logging
    and error handling.
    '''    
    log = None
    writes = 0
    reads = 0
    writeErrors = 0
    readErrors = 0
    
    def __init__(self,server,username,password):
        super(FreebaseSession,self).__init__(server, username, password)
        self.log = logging.getLogger('FreebaseSession')
    
    def fbRead(self, query):
        #log.debug([  '  Read query = ', query])
        try:
            self.reads += 1
            response = self.mqlread(query)
        except MetawebError,e:
            # TODO - Is the retryable?  Wait and retry
            # if not retryable throw exception
            self.readErrors += 1
            self.log.error('**Freebase query MQL failed (%d/%d errors/attempts): %s\nQuery = %s\n' % (self.readErrors, self.reads, repr(e),repr(query)) )
            return None
#       log.debug([ '    Response = ',response])    
        return response

    def fbWrite(self, query):
        #log.debug(['  Write query = ', query])
        try:
            self.writes += 1
            response = self.mqlwrite(query)
        except MetawebError,e:
            # TODO - Is the retryable?  Wait and retry
            # if not retryable throw exception
            # Don't retry quota problems - /api/status/error/mql/access Too many writes
            # retry 503 Internal server error
            # bad request - probably means cookie expired or server rebooted requiring new login
            # timeout - retry?  how many times?  how quickly?
            msg = e.args[0]
            # Huge hack!  Why do we have to do string parsing to find an error code?
            if msg.find('/api/status/error/auth') > 0:
                self.log.warn('Authentication error on MQL write - attempting to login again %s\n',repr(e))
                self.login()
                try:
                    response = self.mqlwrite(query)
                    return response
                except MetawebError, e2:
                    pass # nested exception - fall through to standard error handling
            self.writeErrors += 1
            self.log.error('**Freebase write MQL failed (%d/%d failures/attempts): %s\nQuery = %s\n' % (self.writeErrors, self.writes, repr(e),repr(query)) )
            return []
#       log.debug([ '    Response = ',response])    
        return response
    
    def fbQueryName(self,name,subquery):
        '''Query Freebase for a topic with the given name (including aliases)'''
        query = {'/type/reflect/any_value' : [{'lang' : '/lang/en',
                                            'link|=' : ['/type/object/name',
                                                        '/common/topic/alias'],
                                            'type' : '/type/text',
                                            'value' : name}],
                'name' : None, # return actual formal name of topic
                'id' : None,
                'guid' : None
        }
        query.update(subquery)
        return self.fbRead([query])
    
    
    def queryTypeAndName(self, type, names, createMissing = False):
        '''Query server for given type(s) by name(s) in name or alias field.  
        Return list of GUIDs which match single type.
        If "type" parameter is a list, look at all types and return list of tuples containing
        (guid,[list of matching types]).  createMissing may not be used for multiple types.
        Return empty list if none of the names are found.  Ambiguous matches are not returned.'''
            
        if not names:
            return []
    
        if isinstance(type,list):
            typeq={'type|=':type,'type':[]}
        else:
            typeq={'type':type}
        
        ids = []
        for name in names:
            results = self.fbQueryName(name,typeq)
            if not results:
                self.log.debug(' '.join(['Warning: name not found', name, 'type:', repr(type)]))
                if createMissing:
                    if isinstance(type,str):
                        guid = self.createTopic(name, [type])
                        if guid:
                            ids.append(guid)
                            self.log.info('Created new topic ' + str(guid) + '  ' + name)
                        else:
                            self.log.error('Failed to create new entry ' + name + ' type: ' + type)
                    else:
                        self.log.error('Cannot create topic when searching for multiple types')
            elif len(results) == 1:
                guid = results[0]['guid']
                if isinstance(type,str):
                    ids.append(guid)
                else:
                    ids.append((guid,results[0]['type']))
    #            log.debug(['                                                          found ', name])
            else:
                self.log.warn('Non-unique name found for unique lookup ' + name +' type: ' + repr(type)) 
                # TODO We could create a new entry here to be manually disambiguated later
    #            if createMissing:
    #                guid = self.createTopic(name, type)
    #                if guid:
    #                    ids.append(guid)
    #                    log.info('Created new topic which may need manual disambiguation ', guid, '  ', name)
    #                else:
    #                    log.error('Failed to create new entry ', name, ' type: ', type)
        return ids

           
    def createTopic(self, name, types):
        common = "/common/topic"
        if not common in types: # make sure it's a topic too
            types.append(common)
        query = {'create': 'unconditional', # make sure you've checked to be sure it's not a duplicate
                 'type': types,
                 'name' : name,
                 'guid' : None
                 }
        response = self.fbWrite(query)
        guid = response['guid']
        return guid

#    def freeqSubmit(self, triples, graphport='sandbox', comment=None,
#                    tool_id='/guid/9202a8c04000641f800000001378d774'):
#        """do a mql write. For a more complete description,
#        see http://www.freebase.com/view/en/api_service_mqlwrite"""
#
#        query = {}
#        query['user']=''
#        query['action_type']='LOAD_TRIPLE'
#        query['operator']='/user/spreadsheet_bot'
#        query['check_params']=False
#        query['graphport']=graphport
#        query['mdo_info']={"software_tool":tool_id,"name":comment}
#        query['payload']=triples
#        
#        qstr = json.dumps(query, separators=SEPARATORS)
#        
#        self.log.debug('FREEQSUBMIT: %s', qstr)
#        
#        service = '/freeq/spreadsheet'
#        
#        self.log.info('%s: %s',
#                      service,
#                      query)
#
#        r = self._httpreq_json(service, 'GET')
#        
#        headers = {'Referer' : 'http://data.labs.freebase.com/loader/',
#                   'Origin' : 'http://data.labs.freebase.com'}
#        
#        r = self._httpreq_json(service, 'POST',
#                               form=query, headers=headers)
#        
#        self.log.debug('FREEQSUBMIT RESP: %r', r)
#        return self._mqlresult(r)
    
if __name__ == "__main__":
    session = FreebaseSession('www.sandbox-freebase.com','tfmorris','password')
    result = session.fbWrite({"create":"unconditional","guid":None})
        