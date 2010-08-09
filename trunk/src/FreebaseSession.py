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
_log = logging.getLogger('FreebaseSession')

def getSessionFromArgs():
    '''Convenience method to create a Freebase session using the username, 
    password, and host in the command line arguments.
    Session is NOT logged in on return.'''
    
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", help="Freebase username")
    parser.add_option("-p", "--password", dest="pw", help="Freebase password")
    parser.add_option("-s", "--host", dest="host", help="service host", default = 'api.sandbox-freebase.com')
   
    (options, args) = parser.parse_args()

    user = options.user
    pw = options.pw
    host = options.host
    
    # TODO not sure this is a good idea...
#    if not pw:
#        pw = getpass.getpass()

    _log.info( 'Host: %s, User: %s' % (host, user))
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
    deferred = True # Convert MQL to triples for later writing with triple loader
    triples = []
    
    
    def __init__(self,server,username,password):
        super(FreebaseSession,self).__init__(server, username, password)
        self.log = logging.getLogger('FreebaseSession')
        self.triples = []
        self.encoder = json.JSONEncoder()
    
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

    def triple(self, subject, predicate, object):
        return self.encoder.encode({'s':subject,'p':predicate,'o':object})

    def getType(self,query):
        # loop through all types and return last one
        return query.type

    def expandProperty(self,type,prop):
        propMap = {'/type/object/type' : 'type',
                   '/type/object/name' : 'name',
                   '/type/object/id'   : 'id'
                   }
        if prop[0] != '/':
            prop = type + '/' + prop;
        if propMap[prop]:
            prop=propMap[prop]

    def formatObject(self, object):
        # handle CVTs and any other special requirements
        operation = object.connect
        if operation == 'update': # ?? can't handle??
            pass
        elif operation == 'insert': #OK
            pass
        else:
            pass
        operation = object.create
        if operation == 'unless_connected':
            pass
        elif operation == 'always':
            pass
        else:
            pass

        # 'lang':'/lang/en' - OK, noop
        # 'type':'/type/text' - OK, string literal
        return object

    def getId(self,query):
        if query.mid:
            return query.mid
        elif query.guid:
            return query.guid
        elif query.id:
            return query.id
        return None

    def triplify(self,query):
        triples = []
        subject = self.getId(query);

        type = self.getType(query)
        for k,v in query.iteritems():
            pn = self.expandProperty(k)
            pv = self.formatObject(v) # can expand to multiple triples and/or CVT
            triples.append(self.encoder.encode(self.triple(subject,pn,pv)))

        return '\n'.join(triples)

    def fbWriteLater(self,query):
        t = self.triplify(query)
        _log.debug(t)
        self.triples.extend(t)
        return '' # TODO return success response

    def fbWriteFlush(self):
        '''Submit all pending triples for processing'''
        payload= '\n'.join(self.triples)
        # login right before submission to close window where server reboots can affect us
        session.login()
        resp,body = session.tripleSubmit(triples=payload,
                                         job_comment='A job comment here',
                                         data_comment="%d triples" % len(self.triples))

        # if successful
        self.triples = []

        print resp,body

    def fbWrite(self, query):
        #log.debug(['  Write query = ', query])
        if self.deferred:
            return self.fbWriteLater(self,query)
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

    def tripleSubmit(self, triples, graphport='sandbox', job_comment=None, data_comment=None,
                    tool_id='/guid/9202a8c04000641f800000001378d774'):
        """do a mql write. For a more complete description,
        see http://www.freebase.com/view/en/api_service_mqlwrite"""
        
        # Huge hack to swap out service URL so we can use session login cookie
        domain = 'data.labs.freebase.com'
        # copy cookies over to new domain
        for name,c in self.cookiejar._cookies['api.freebase.com']['/'].items():
            c.domain=domain
            self.cookiejar.set_cookie(c)

        service_url = self.service_url
        self.service_url="http://" + domain + "/"
        try:
        
            form = {
            'action_type':'LOAD_TRIPLE',
    #        'user' :'',
    #        'operator' : '/user/spreadsheet_bot',
            'check_params' : False, # prevents 'not a valid bot user' authentication error
#            'pod' : graphport, # obsolete?
            'comments' : job_comment,
            'graphport':graphport,
            'mdo_info' : {"software_tool":tool_id,
                          "info_source":"/wikipedia/en/wikipedia",
                          "name":data_comment},
            'payload':triples
            }
             
#            self.log.debug('FREEQSUBMIT: %s', form)
            
            service = '/triples/data/'
#            headers = {'Accept' : 'text/plain'}            
            resp,body = self._httpreq(service, 'POST',
                                   form=form)#, headers=headers)
            
            self.log.debug('FREEQSUBMIT RESP: %r', resp)
            self.log.debug('FREEQSUBMIT RESP: %r', body)
        finally:
            self.service_url = service_url

        #self.log.info('result: %s', Delayed(logformat, r))
        return resp,body
#        return self._mqlresult(r)
    
    
if __name__ == "__main__":
    session = FreebaseSession('api.sandbox-freebase.com','tfmorris','password')
    result = session.fbWrite({"create":"unconditional","guid":None})
        
