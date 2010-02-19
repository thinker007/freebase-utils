'''
Load missing NRIS ids into Freebase using FreeQ/Triple Loader.  Input is a file of
Wikipedia ID / NRIS ref num tuples.  This file is separately generated using 
template/infobox data from BFG.  Program creates triples to assign keys for the
reference number and add appropriate types to the existing topics.

Created on Jan 14, 2010

@author: Tom Morris <tfmorris@gmail.com>
@copyright: Copyright 2010 Thomas F. Morris
@license: Eclipse Public License (EPL) v1
'''

import json
import logging
from fileinput import FileInput

from FreebaseSession import FreebaseSession, getSessionFromArgs

def main():
    file = FileInput('id-missing.txt') # 4 space separated columns 1 & 2 junk, 3 - NRIS ref#, 4 - FB ID
    session = getSessionFromArgs();
#    status = session.mqlwrite([{"id":"/guid/9202a8c04000641f800000001378d774",  "type":{"id":"/common/topic","connect":"insert"}}])
    triples = []
    count = 0
    for line in file:
        fields = line.strip().split(' ')
        id = fields[3]
        refno = fields[2]
        triple = {'s':fields[3], 'p': 'key','o':'/base/usnris/item/'+fields[2]}
        triples.append(json.JSONEncoder().encode(triple))
        triple.update({'p':'type','o':'/base/usnris/nris_listing'})
        triples.append(json.JSONEncoder().encode(triple))
        triple.update({'p':'type','o':'/base/usnris/topic'})
        triples.append(json.JSONEncoder().encode(triple))
        triple.update({'p':'type','o':'/protected_sites/listed_site'})
        triples.append(json.JSONEncoder().encode(triple))
    payload= '\n'.join(triples)
#    payload=json.JSONEncoder().encode({'s':'/guid/9202a8c04000641f800000001378d774','p':'alias','o':'Le remplisseur de Thomas','lang':'/lang/fr'})
#    print payload
    session.login() # login right before submission to close window where server reboots can affect us
    resp,body = session.tripleSubmit(triples=payload,job_comment='Trying it again',data_comment="%d topics from U.S. Register of Historic Places" % len(triples))
    print resp,body

if __name__ == '__main__':
    main()