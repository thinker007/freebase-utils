'''
Created on Jul 24, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: AGPLv3 (contact author for other license options)
'''

import logging
import re

suffixRe = re.compile('(J|j)r|Sr|II|III|IV|(S|s)econd|2nd|Inc|MD|M\.D\.')
prefixRe = re.compile('Dr|Capt|Col|Gen|Mrs|Rev')

log = logging.getLogger('names')

def isNameSeries(name):
    return name.find('&') >=0 or name.find(' and ' ) >=0;

def normalizePersonName(name):
    # Check for firms/partnerships first and skip them
    if isNameSeries(name):
        return name

    # Special case for multiple significant person names
    if name.lower().strip() == 'multiple':
        return ''
    
    # Strip various forms of et al
    # TODO use regex
    for s in [', et al.', ',et al.', ', et al', ',et al', ', et.al.' ]:
        name = name.replace(s, '')
            
    pieces = name.split(',')
    if len(pieces) == 1:
        return name
    elif len(pieces) == 2:
        return ' '.join([pieces[1].strip(), pieces[0].strip()])
    elif len(pieces) == 3:
        # TODO Do we want John Smith Jr or John Smith, Jr ?
        return ' '.join([pieces[1].strip(), pieces[0].strip()]) + ', ' + pieces[2].strip()
    else:
        # TODO: More cases
        return name
        
        
def normalizeName(siteName):
    '''Attempt to undo various name transformations'''

    # Get rid of (Boundary Increase), (schooner), etc
    siteName = stripParen(siteName)

    # Convert double hyphen style m-dash to single hyphen
    while siteName.find('--') >= 0:
        siteName = siteName.replace('--', '-')
        
    commas = siteName.count(', ')
    if commas == 0:
        return siteName
       
#   log.debug('Old name ' + name)
    # TODO - Split on commas and remove spaces separately
    pieces = siteName.split(', ')

    # If it looks like a series, leave it as is
    if ' and ' in pieces[commas].lower() or '&' in pieces[commas]:
        return siteName
    
    if commas == 1:
        result = ' '.join([pieces[1], pieces[0]])
    elif commas == 2:
        result = ' '.join([pieces[1], pieces[0], pieces[2]])
    elif commas == 3:
        if not suffixRe.search(pieces[2]) == None:
            ## TODO: ?Generalize to keep all commas except for 1st??
            result = ' '.join([pieces[1], ', '.join([pieces[0], pieces[2], pieces[3]])]) 
#            log.debug(['  Converted ',siteName,' to ',result])
        elif not prefixRe.search(pieces[2]) == None:
            result = ' '.join([pieces[2], pieces[0], pieces[1], pieces[3]])
        elif not prefixRe.search(pieces[1]) == None:
            result = ' '.join([pieces[1], pieces[2], pieces[0], pieces[3]])
        elif len(pieces[2]) == 2 and pieces[2][1:] == '.':
            # Handle lone middle initial
            result = ' '.join([pieces[1], pieces[2], pieces[0], pieces[3]])
        else:
#            log.debug(['**no RE match for ', siteName])
            result = siteName
    else:
#        log.debug(['**no new name for ', siteName])
        result = siteName
    # TODO: What other cases do we need to handle?
    # Pierce, Capt, Mial, Farm
    # Winslow, Luther, Jr., House
    # Little, Arthur D., Inc., Building
    # Cutter, Second, A. P., House
    # Olmsted, Frederick Law, House, National Historic Site
    # Lindsay, James-Trotter, William, House
    # Lansburgh, Julius, Furniture Co., Inc.

    return result


def stripParen(s):
    '''Strip a (single) parenthetical expression from a string'''
    p1 = s.find('(')

    if p1 >= 0:
        p2 = s.find(')', p1)
        if p2 >= 0:
            s = s[:p1].strip() + ' ' + s[p2+1:].strip()
        else:
            log.warn('Failed to find closing paren ' + s)
            s = s[:p1].strip()
    return s



def __test():
    print normalizeName("Smith, Jones, and Wiggins (huh)")
    print isNameSeries("Smith, Jones, and Wiggins")
    print normalizePersonName("Morris, Thomas F.")
    print normalizePersonName("Dr. T. John Smith (editor)")

if __name__ == '__main__':
     __test()