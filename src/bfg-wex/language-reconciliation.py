'''
Utility to check for and reconcile duplicate Human Language entries which
appear to come from new Wikipedia articles being created for languages and
not getting reconciled on import into Freebase.

Created on Aug 14, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2009 Thomas F. Morris
@license Eclipse Public License v1
'''

from bfg_session import BfgSession
from freebase.api import HTTPMetawebSession
from datetime import datetime
import logging

def main ():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
    log = logging.getLogger('language-recon')
    log.setLevel(logging.DEBUG)
    log.info("Beginning at %s" % str(datetime.now()))

    bfgSession = BfgSession()
    fbSession = HTTPMetawebSession('http://www.freebase.com')
    # Get a list of template calls to the Language Infobox template
    query = {'path':'wex-index',
             'sub': '',
             'pred':'wex:tc/template',
             'obj':'Template:Infobox Language',
             'limit': 10000
             }
    result = bfgSession.query(query)
    log.info('Number of languages on wikipedia with infoboxes = ' + str(len(result)))

    # Fetch each template call and look for language name and iso3 code
    for r in result:
        subject = r.s
        # Find inbound subject of which this is the object 
        # (ie main article which is calling template) 
        result = bfgSession.query({'path':'wex-index',
                                'pred' : 'wex:a/template_call',
                                'obj': subject})
        if len(result) == 1:
            r = result[0]
            if r.s.startswith('wexen:wpid/'):
                wpid = r.s[11:]
                result = bfgSession.query({'path':'wex-index',
                                        'sub': subject})

                # Extract interesting parameters (name and iso 639 codes)
                code = {}
                name = ''
                for p in result:
                    if 'param' in p:
                        if p['param'].startswith('iso'):
                            v = p['o']
                            if v!='none' and v!='-' and v !='?':
                                code[p['param']] = v
                        if p['param'] == 'name':
                            name = p['o']

                # Now query Freebase using the Wikipedia parameters to see how
                # many different topics they resolve to
                ids = {}
                for i in range(0,4):
                    query = [{'id': None,
                      "name":       None,
                      "key": [{
                        "namespace": "/wikipedia/en_id",
                        "value":     None
                      }],
                      "type":          "/language/human_language",
                      "iso_639_1_code": None,
                      "iso_639_3_code": None,
                      "iso_639_2_code": None
                    }]
                    if i == 0:
                        query[0]['key'][0]['value'] = wpid
                        key = 'wpid'
                        value = wpid
                    else:
                        key = 'iso'+str(i)
                        value = code[key]
                        if not key in code:
                            continue
                        query[0]['iso_639_'+str(i)+'_code'] = code[key]

                    fbResult = fbSession.mqlread(query)

                    for fbr in fbResult:
                        if not fbr.id in ids:
                            ids[fbr.id] = {}
                        ids[fbr.id][key] = value

                if len(ids) == 0:
                    # The following will fail for unicode names when run from the console (or debugger)
                    # Send the output to a log file instead
                    log.warn(name + ' no Freebase topic with WPID =' + wpid + ' or codes ' + repr(code))
                elif len(ids) > 1:
                    # The following will fail for unicode names when run from the console (or debugger)
                    # Send the output to a log file instead
                    log.warn(name + ' multiple Freebase topics resolved for WPID=' + wpid + ' codes=' + repr(code) + ' ' + repr(ids))
                elif len(ids.items()[0]) != 4:
                    # The following will fail for unicode names when run from the console (or debugger)
                    # Send the output to a log file instead
                    log.warn(name + ' keys missing for WPID= ' + wpid + ' codes=' + repr(code) + ' ' + repr(ids))
                else:
                    log.debug(name + ' OK - WPID=' + wpid + ' ' + repr(code))
                continue
        log.error('** failed to find WPID for ' + repr(result))
    log.info("Done at %s" % str(datetime.now()))

    
if __name__ == '__main__':
    main()