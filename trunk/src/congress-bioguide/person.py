# coding: utf-8
'''
Created on Mar 7, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
'''

import logging
import string

class Person(object):
    '''
    classdocs
    '''
    PREFIX_MULTI_WORD_SURNAME = ['de', 'des', 'del', 'van', 'von', 'te', 'ten', 'ter' ]

    _given_names = [] # All given names in the order and with the punctuation normally used
    _nickname = None # Often a shortened or familiar form of one of the given names, but not necessarily
    _family_name = None # May not be used in all cases e.g. patronynmic societies
    _birth_date = None
    _death_date = None
    _title = None # Dr., Rev., Gen. , General, etc TODO - needs more work
    _name_suffix = None # Generational indicator (Jr., III) or postnominals - TODO split?
    _gender = None # 'M', 'F', or 'O' (Other covers all intersexed variants)
    _religion = None # TODO This needs to be modeled as more than a simple value

    def __init__(self):
        '''
        Constructor
        '''
        self._log = logging.Logger('person', logging.debug)

    def parse_multi_word_surname(self, pieces):
        for p in pieces:
            if p in self.PREFIX_MULTI_WORD_SURNAME:
                self._family_name = ' '.join(pieces[pieces.index(p):])
                self._given_names = pieces[:pieces.index(p)]


    def parse_title(self, pieces):
        '''Parse and save anything that looks like a title'''
        # For now it's just any abbreviation that doesn't look like an initial
        if pieces[0].endswith('.') and len(pieces[0]) > 2:
            self._title = pieces[0]
            pieces.remove(pieces[0])
        #TODO - Compare against table of common titles (fetch from Freebase?)

    def extract_nickname(self, name_string):
        '''Extract and store any nickname and return remainder of string'''
        
        # Nickname in quotes
        if name_string.count('"') == 2:
            pieces = name_string.split('"')
            name_string = pieces[0] + pieces[2]
            self._nickname = pieces[1]
        # Nickname in parentheses
        
        p1 = name_string.find('(')
        p2 = name_string.find(')', p1)
        if p1 >= 0 and p2 >= 0:
            self._nickname = name_string[p1 + 1:p2]
            name_string = name_string[:p1].strip() + ' ' + name_string[p2 + 1:].strip()
        
        return name_string

    def count_case(self, words):
        '''Return a list of counts of words with upper, lower, and title case, in that order'''
        count = [0, 0, 0]
        for p in words:
            if p.isupper():
                count[0] += 1
            if p.islower():
                count[1] += 1
            if p.istitle():
                count[2] += 1
        # TODO - This could be used to identify SURNAMES (but isn't)
#        if counts[0] > 0 and counts[0] != len(pieces):
#            # use all upper case pieces as surname hint (as long as the whole name isn't upper case)
#            # convert to title case
#            pass
#        if counts[1] > 0 and counts[1] != len(pieces):
#            # user lower case pieces as hints for van der Wald, etc ?
#            pass
        return count
    
    def guess_culture(self, name):
        # TODO Identify culture / class for name
        return 'usa'

    def parse(self, name_string, culture = 'guess'):
        '''
        Attempt to parse a name string into its component parts.
        NOTE: This is inherently unreliable, so structured data should be used
            when available.
        '''

        if culture == 'guess':
            culture = self.guess_culture(name_string)
        
        if culture == 'usa':
            self.parse_usa(name_string)
#        elif culture == 'esp':
#            pass
        else:
            # error
            self._log.error('Unknown cultural category for name')
            
    def parse_usa(self, name_string):
            commas = name_string.split(',')
            if len(commas) == 2:
                # could be last, first or first last, Jr.
                if commas[1].find('.') > 0:
                    self._name_suffix = commas[1].strip()
                    name_string = commas[1]
                else:
                    self._family_name = commas[0]
                    self._given_names = commas[1]
                    return
            elif len(commas) > 2:
                self._log.error('Can not parse strings with more than one comma')
                return
            
            name_string = self.extract_nickname(name_string)

            pieces = name_string.split()
            
            self.normalize_periods(pieces)

            self.parse_title(pieces)
            
#            counts = self.count_case(pieces)

            if self.parse_multi_word_surname(pieces):
                return

            self._family_name = pieces[len(pieces)-1]
            self._given_names = pieces[:len(pieces)-1]

            
    def normalize_periods(self, list):
        '''Split any items which have embedded periods (not at the end)'''
        for item in list:
            if '.' in item:
                i = item.index('.')
                if i >= 0 and i < len(item)-1:
                    pos = list.index(item)
                    new = [s + '.' for s in item.split('.') if s != '']
                    for n in new:
                        list.insert(pos, n)
                        pos += 1
                    list.remove(item)
    
    def normalize_quotes(self, text):
        ''''
        Normalize all types of quote characters to an ASCII double quote
        (really a string util, but we use it to make sure we can match nicknames)
        '''
        quote_chars = u'\u00ab\u00bb\u2018\u2019\u201a\u201b\u201c\u201d\u201e\u201f\u275b\u275c\u275d\u275e\u301d\u301e\uff02'
        for c in quote_chars:
            text = text.replace(c,u'"') # very inefficient!
        return text
        # The following doesn't work
#        lut = dict((c,u'"') for c in quote_chars)
#        return text.translate(lut)

    def format_name(self, options = []):
        '''
        Return name formatted name with specified components included.
        '''
        result = ''
        if 'given' in options:
            result += " ".join(self._given_names)
        elif 'first' in options or 'fi' in options:
            if len(self._given_names) > 0:
                if 'fi' in options:
                    result += self._given_names[0][0] + '.'
                else:
                    result += self._given_names[0]
        if 'mi' in options and len(self._given_names) > 1:
            result += ' ' + (' '.join([n[0] + '.' for n in self._given_names[1:] ]))
        if 'middle' in options and len(self._given_names) > 1:
            result += ' ' + (' '.join([n for n in self._given_names[1:] ]))
        if self._nickname and 'nick' in options:
            if 'given' in options or 'first' in options or 'fi' in options:
                result += ' "' + self._nickname + '"'
            else:
                result += self._nickname 
        if self._family_name:
            result = result + ' ' + self._family_name
        if self._name_suffix and 'suffix' in options:
            result = result + ', ' + self._name_suffix
        if self._title and 'title' in options:
            result = result + ' ' + self._title
        return result.strip()
                 
    def format_full_name(self):
        '''
        Return name in preferred format
        '''
        return self.format_name(['title', 'given','nick', 'suffix' ])
    
    def format_name_with_dates(self):
        dob = '?'
        if self._birth_date:
            dob = self._birth_date
        dod = ''
        if self._death_date:
            dod = self._death_date
        return '%s (%s-%s)' % (self.format_full_name(), dob, dod)
    
    def format_all_names(self):
        '''
        Return a list of all possible name strings in order of preference
        for the culture (not the individual)
        '''
        result = []
        # nickname last
        result.append(self.format_name(['first']))
        if self._nickname:
            result.append(self.format_name(['nick']))
        result.append(self.format_name(['first', 'mi']))
        result.append(self.format_name(['first', 'mi', 'suffix']))
        result.append(self.format_name(['fi', 'mi', 'suffix']))
        result.append(self.format_name(['fi', 'middle', 'suffix']))
        result.append(self.format_name(['middle', 'suffix'])) # TODO - low priority - do in 2nd phase?
        result.append(self.format_name(['title', 'first', 'mi', 'suffix']))
        result.append(self.format_name(['first', 'mi', 'nick']))
        result.append(self.format_name(['first', 'mi', 'nick', 'suffix']))
        result.append(self.format_name(['title', 'first', 'mi', 'nick', 'suffix']))
        result.append(self.format_name(['title', 'given', 'nick', 'suffix']))
        return set(result)
        
def main():
    names = ['Jan van der Welt',
             'Jean-Paul de la Rose', 
             'Thomas F. Morris', 
             'James "Whitey" Bulger',
             'James “Whitey” Bulger',
             'Dr. Billy Bob Thornton',
             'T. Boone Pickens',
             'SMITH, James, Jr.',
             'Smith, James',
             'John J.T. Thomas',
             'Clarence J. Brown, Jr.',
             ]
    for n in names:
        p = Person()
        n = p.normalize_quotes(n)
        p.parse(n)
        print n, '-', p._family_name, p._given_names, p._nickname, p._title

if __name__ == '__main__':
    main()
            