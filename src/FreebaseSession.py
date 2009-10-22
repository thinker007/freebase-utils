'''
Created on Aug 8, 2009

@author: Tom Morris <tfmorris@gmail.com>
@license: Eclipse Public License
@copyright: 2009 Thomas F. Morris
'''

import logging
from optparse import OptionParser
import getpass

from freebase.api import HTTPMetawebSession, MetawebError


def getSessionFromArgs():
    '''Convenience method to create a Freebase session using the username, 
    password, and host in the command line arguments.
    Session is NOT logged in on return.'''
    
    log = logging.getLogger('FreebaseSession')
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", help="Freebase username", default='tfmorris')
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
    return HTTPMetawebSession(host, username=user, password=pw)

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
    
if __name__ == "__main__":
    session = FreebaseSession('www.sandbox-freebase.com','tfmorris','password')
    result = session.fbWrite({"create":"unconditional","guid":None})
        