'''
BFG (Big F*ing Graph) Session Management
Created on Aug 14, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2009 Thomas F. Morris
@copyright 2007-2009 Metaweb Technologies (see notice below)
@license Eclipse Public License v1
'''

# ==================================================================
# Copyright (c) 2007, Metaweb Technologies, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY METAWEB TECHNOLOGIES AND CONTRIBUTORS
# ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL METAWEB
# TECHNOLOGIES OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ====================================================================

__version__ = '0.5'

from freebase.api.httpclients import Httplib2Client, Urllib2Client, UrlfetchClient
from freebase.api.session import Delayed, logformat,HTTPMetawebSession,MetawebError
import logging

try:
    from urllib import quote_plus as urlquote_plus
except ImportError:
    from urlib_stub import quote_plus as urlquote_plus
    
try:
    import jsonlib2 as json
except ImportError:
    try:
        import json
    except ImportError:
        try:
            import simplejson as json
        except ImportError:
            try:
                # appengine provides simplejson at django.utils.simplejson
                from django.utils import simplejson as json
            except ImportError:
                raise Exception("unable to import neither json, simplejson, jsonlib2, or django.utils.simplejson")


# Check for urlfetch first so that urlfetch is used when running the appengine SDK
try:
    import google.appengine.api.urlfetch
    http_client = UrlfetchClient
except ImportError:
    try:
        import httplib2
        http_client = Httplib2Client
    except ImportError:
        import urllib2
        httplib2 = None
        http_client = Urllib2Client

def urlencode_weak(s):
    return urlquote_plus(s, safe=',/:$')


# from http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/361668
class attrdict(dict):
    """A dict whose items can also be accessed as member variables.
    
    >>> d = attrdict(a=1, b=2)
    >>> d['c'] = 3
    >>> print d.a, d.b, d.c
    1 2 3
    >>> d.b = 10
    >>> print d['b']
    10
    
    # but be careful, it's easy to hide methods
    >>> print d.get('c')
    3
    >>> d['get'] = 4
    >>> print d.get('a')
    Traceback (most recent call last):
    TypeError: 'int' object is not callable
    """
    def __init__(self, *args, **kwargs):
        # adds the *args and **kwargs to self (which is a dict)
        dict.__init__(self, *args, **kwargs)
        self.__dict__ = self


class BfgSession(object):
    '''
    classdocs
    '''


    def __init__(self, service_url = 'http://data.labs.freebase.com'):
        '''
        Constructor
        '''
        self.log = logging.getLogger()
        self.service_url = service_url
        self._http_request = http_client(None, self._raise_service_error)

    
    def treequery(self, query):
        return self._query('/bfg/treequery', query)
    
    def query(self, query):
        return self._query('/bfg/query', query)
    
    def _query(self, service, query):
        """Query the Big F*ing graph.  The single argument is a dictionary
        containing query parameters which will be converted to HTTP form
        parameters and submitted.
        
        Return is a list with a dictionary per result from the query.
        """

#        self.log.info('%s: %s',
#                      service,
#                      Delayed(logformat, query))

        subq = dict(query=query, escape=False)
#        qstr = '&'.join(['%s=%s' % (urlencode_weak(unicode(k)), urlencode_weak(unicode(v)))
#                             for k,v in query.items()])  
        r = self._httpreq_json(service, form=query)
        
        return self._result(r)
        pass


        
    def _httpreq(self, service_path, method='GET', body=None, form=None,
                 headers=None):
        """
        make an http request to the service.
        
        form arguments are encoded in the url, even for POST, if a non-form
        content-type is given for the body.
        
        returns a pair (resp, body)
        
        resp is the response object and may be different depending
        on whether urllib2 or httplib2 is in use?
        """
        
        if method == 'GET':
            assert body is None
        if method != "GET" and method != "POST":
            assert 0, 'unknown method %s' % method
        
        url = self.service_url + service_path
        
        if headers is None:
            headers = {}
        else:
            headers = _normalize_headers(headers)
        
        # this is a lousy way to parse Content-Type, where is the library?
        ct = headers.get('content-type', None)
        if ct is not None:
            ct = ct.split(';')[0]
        
        if body is not None:
            # if body is provided, content-type had better be too
            assert ct is not None
        
        if form is not None:
            qstr = '&'.join(['%s=%s' % (urlencode_weak(unicode(k)), urlencode_weak(unicode(v)))
                             for k,v in form.items()])
            if method == 'POST':
                # put the args on the url if we're putting something else
                # in the body.  this is used to add args to raw uploads.
                if body is not None:
                    url += '?' + qstr
                else:
                    if ct is None:
                        ct = 'application/x-www-form-urlencoded'
                        headers['content-type'] = ct + '; charset=utf-8'
                    
                    if ct == 'multipart/form-encoded':
                        # TODO handle this case
                        raise NotImplementedError
                    elif ct == 'application/x-www-form-urlencoded':
                        body = qstr
            else:
                # for all methods other than POST, use the url
                url += '?' + qstr

        
        # assure the service that this isn't a CSRF form submission
        headers['x-metaweb-request'] = 'Python'
        
        if 'user-agent' not in headers:
            headers['user-agent'] = 'python freebase.api-%s' % __version__
        
        ####### DEBUG MESSAGE - should check log level before generating
        loglevel = self.log.getEffectiveLevel()
        if loglevel <= 20: # logging.INFO = 20
            if form is None:
                formstr = ''
            else:
                formstr = '\nFORM:\n  ' + '\n  '.join(['%s=%s' % (k,v)
                                              for k,v in form.items()])
            if headers is None:
                headerstr = ''
            else:
                headerstr = '\nHEADERS:\n  ' + '\n  '.join([('%s: %s' % (k,v))
                                                  for k,v in headers.items()])
            self.log.info('%s %s%s%s', method, url, formstr, headerstr)
        
        # just in case you decide to make SUPER ridiculous GET queries:
        if len(url) > 1000 and method == "GET":
            method = "POST"
            url, body = url.split("?", 1) 
            ct = 'application/x-www-form-urlencoded'
            headers['content-type'] = ct + '; charset=utf-8'
           
        return self._http_request(url, method, body, headers)
    
    def _result(self, r):
#        self._check_mqlerror(r)
        
        self.log.info('result: %s', Delayed(logformat, r))
        
        return r
        
    def _raise_service_error(self, url, status, ctype, body):
        
        is_jsbody = (ctype.endswith('javascript')
                     or ctype.endswith('json'))
        if str(status) == '400' and is_jsbody:
            r = self._loadjson(body)
            msg = r.messages[0]
            raise MetawebError(u'%s %s %r' % (msg.get('code',''), msg.message, msg.info))
        
        raise MetawebError, 'request failed: %s: %s\n%s' % (url, status, body)
    
    def _httpreq_json(self, *args, **kws):
        resp, body = self._httpreq(*args, **kws)
        return self._loadjson(body)
    
    def _loadjson(self, json_input):
        # TODO really this should be accomplished by hooking
        # simplejson to create attrdicts instead of dicts.
        def struct2attrdict(st):
            """
            copy a json structure, turning all dicts into attrdicts.
            
            copying descends instances of dict and list, including subclasses.
            """
            if isinstance(st, dict):
                return attrdict([(k,struct2attrdict(v)) for k,v in st.items()])
            if isinstance(st, list):
                return [struct2attrdict(li) for li in st]
            return st
        
        if json_input == '':
            self.log.error('the empty string is not valid json')
            raise MetawebError('the empty string is not valid json')
        
        try:
            r = json.loads(json_input)
        except ValueError, e:
            self.log.error('error parsing json string %r' % json_input)
            raise MetawebError, 'error parsing JSON string: %s' % e
        
        return struct2attrdict(r)


