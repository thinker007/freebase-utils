'''
Created on Aug 8, 2009

@author: Tom Morris <tfmorris@gmail.com>
@license: Eclipse Public License
@copyright: 2009 Thomas F. Morris
'''

from freebase.api import HTTPMetawebSession, MetawebError

import logging

class FreebaseSession(HTTPMetawebSession):
    '''
    Extended version of HTTPMetawebSession to allow wrappers with our own logging
    and error handling.
    '''    
    log = None
    
    def __init__(self,server,username,password):
        super(FreebaseSession,self).__init__(server, username, password)
        self.log = logging.getLogger('FreebaseSession')
    
    def fbRead(self, query):
        #log.debug([  '  Read query = ', query])
        try:
            response = self.mqlread(query)
        except MetawebError,e:
            # TODO - Is the retryable?  Wait and retry
            # if not retryable throw exception
            self.log.error('**Freebase query MQL failed : ' + repr(e) + '\nQuery = ' + repr(query))
            return None
#       log.debug([ '    Response = ',response])    
        return response

    def fbWrite(self, query):
        #log.debug(['  Write query = ', query])
        try:
            response = self.mqlwrite(query)
        except MetawebError,e:
            # TODO - Is the retryable?  Wait and retry
            # if not retryable throw exception
            # Don't retry quota problems - /api/status/error/mql/access Too many writes
            # retry 503 Internal server error
            # bad request - probably means cookie expired or server rebooted requiring new login
            msg = e.args[0]
            # Huge hack!  Why do we have to do string parsing to find an error code?
            if msg.find('/api/status/error/auth'):
                self.log.warn('Authentication error on MQL write - attempting to login again' + repr(e))
                self.login()
                try:
                    response = self.mqlwrite(query)
                    return response
                except MetawebError, e2:
                    pass # nested exception - fall through to standard error handling
            self.log.error('**Freebase write MQL failed : ' + repr(e) + '\nQuery = ' + repr(query))
            return []
#       log.debug([ '    Response = ',response])    
        return response
    
if __name__ == "__main__":
    session = FreebaseSession('www.sandbox-freebase.com','tfmorris','password')
    result = session.fbWrite({"create":"unconditional","guid":None})
        