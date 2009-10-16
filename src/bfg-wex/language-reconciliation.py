'''
Utility to check for and reconcile duplicate Human Language entries which
appear to come from new Wikipedia articles being created for languages and
not getting reconciled on import into Freebase.

Running time: about one hour
Latest results (15 Oct 2009) : 
 missing : 4
 multiple matches: 226  <== duplicate topics which need merging
 missing ISO1/2/3 codes: 3784
 mismatched codes between Freebase and Wikipedia: 0

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

    stats = {}
    stats['missing'] = 0
    stats['multiple'] = 0
    stats['mismatch'] = 0
    stats['missing_code'] = 0
    
    # Fetch each template call and look for language name and iso3 code
    for r in result:
        subject = r.s
        # Find inbound subject of which this is the object 
        # (ie template call section in main article which is calling template) 
        result = bfgSession.query({'path':'wex-index',
                                'pred' : 'wex:a/template_call',
                                'obj': subject})
        if len(result) == 1:
            r = result[0]
            if r.s.startswith('wexen:wpid/'):
                wpid = r.s[len('wexen:wpid/'):]
                # Fetch all triples with this subject
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
                                if (len(v) > 3):
                                    # TODO make this a regex for 2 or 3 alpha characters
                                    log.warning("**Bad ISO lang code for WPID %s:  %s" % (wpid, v))
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
                      "type":          [],
                      "/language/human_language/iso_639_1_code": None,
                      "/language/human_language/iso_639_3_code": None,
                      "/language/human_language/iso_639_2_code": None
                    }]
                    if i == 0:
                        query[0]['key'][0]['value'] = wpid
                        key = 'wpid'
                        value = wpid
                    else:
                        key = 'iso'+str(i)
                        if not key in code:
                            continue
                        value = code[key]
                        query[0]['/language/human_language/iso_639_'+str(i)+'_code'] = value

                    fbResult = fbSession.mqlread(query)

                    for fbr in fbResult:
                        if not '/language/human_language' in fbr.type:
                            log.warn(fbr.id + ' not typed as a /language/human_language')
                        if not fbr.id in ids:
                            ids[fbr.id] = {}
                        ids[fbr.id][key] = value

                # The logging below will fail for unicode names when run from the console (or debugger)
                # Send the output to a log file instead
                if len(ids) == 0:
                    log.warn(name + ' no Freebase topic with WPID =' + wpid + ' or codes ' + repr(code))
                    stats['missing'] += 1
                elif len(ids) > 1:
                    # TODO watch out for multiple infobox templates on the same Wikipedia page
                    # e.g. http://en.wikipedia.org/wiki/index.html?curid=1267995#Akeanon
                    log.warn(name + ' multiple Freebase topics resolved for WPID=' + wpid + ' codes=' + repr(code) + ' ' + repr(ids))
                    stats['multiple'] += 1
                else:
                    fbItem = ids.items()[0]
                    for c in code:
                        if not c in fbItem:
                            log.warn(name + ' missing Freebase code ' + c + '=' + code[c])
                            stats['missing_code'] += 1
                        else:
                            if code[c] != fbItem[c]:
                                log.warn(name + ' code mismatch for ' + c + '=' + code[c] + '(WP)!=(FB)' + fbItem[c])
                                stats['mismatch'] += 1

                #  log.warn(name + ' keys missing for WPID= ' + wpid + ' codes=' + repr(code) + ' ' + repr(ids))
                continue
        log.error('** failed to find WPID for ' + repr(result))
    log.info("Done at %s   %s" % (str(datetime.now()), repr(stats)))
    
if __name__ == '__main__':
    main()