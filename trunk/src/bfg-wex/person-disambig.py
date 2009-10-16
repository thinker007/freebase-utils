'''
Utility to check Wikipedia Human name disambiguation pages 
against Freebase Person topics.

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
             'pred':'', #
             'obj':'Category:Human name disambiguation pages',
             'limit': 26000
             }
    result = bfgSession.query(query)
    log.info('Number of pages in Category:Human name disambiguation pages on wikipedia  = ' + str(len(result)))
    query = {'path':'wex-index',
             'sub': '',
             'pred':'', #
             'obj':'Template:Hndis',
             'limit': 26000
             }
    result = bfgSession.query(query)
    log.info('Number of pages in with Template:Hndis = ' + str(len(result)))
    log.info("Done at %s" % str(datetime.now()))

    
if __name__ == '__main__':
    main()