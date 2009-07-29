# coding: utf-8
'''
Created on Mar 11, 2009

@author: Tom Morris <tfmorris@gmail.com>
@copyright: 2009 Thomas F. Morris
@license: Eclipse Public License v1 http://www.eclipse.org/legal/epl-v10.html
 licensing arrangements)
'''

import codecs
import logging
from math import sqrt

from freebase.api import HTTPMetawebSession, MetawebError
from metaweb import search

from person import Person

SCORE_THRESHOLD = 1.9
SEARCH_RELEVANCE_THRESHOLD = 8.0 # This may change over time

class FbPerson(Person):
    '''
    classdocs
    '''
    

    def __init__(self):
        '''
        Constructor
        '''
        self._id = None
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
        self._log = logging.getLogger('FbPerson')
        self._log.setLevel(logging.DEBUG)

    def name_query(self, name):
        '''
        Construct a query to look up a person in Freebase with the given name
        (or list of name alternatives)
        and return all the info we're interested in.  Most things are only used
        for scoring, rather than constraining the lookup, so they're optional.
        '''
        sub_query = [{'lang' : '/lang/en',
                        'link|=' : ['/type/object/name',
                                    '/common/topic/alias'],
                        'type' : '/type/text'
                        }]

        if '*' in name:
            sub_query[0]['value~='] = name
        elif isinstance(name,list):
            sub_query[0]['value|='] = name
        else:
            sub_query[0]['value'] = name

        query = {'/type/reflect/any_value' : sub_query,
                  't1:type' : '/people/person',
                  't2:type' : '/common/topic',
                  't3:type' : {'name' : '/people/deceased_person', 
                               'optional': True},
                  'id' : None
                  }
        return [self.decorate_query(query)]
    
    def decorate_query(self, query):
        query.update({'type' : [],
                  'name' : None,
                  '/common/topic/alias': [],
                  '/people/person/date_of_birth' :  None,
                  '/people/deceased_person/date_of_death' : None,
                  '/common/topic/article': [{'id': None,'optional' : True}]
                  })
        return query
        
    def query(self, session, queries):
        '''
        Send a batch of queries to Freebase and return the set of *unique*
        results.  Since we're doing lookups on both name and alias, we'll see
        the same topics multiple times.
        '''
        results = []
        ids = []
#        print len(repr(queries)), queries

        # Split queries in chunks to keep URL under length limit
        while len(queries) > 0:
            size = min(4, len(queries))
            query_slice = queries[:size]
            del queries[:size]
            try:
                for query_result in session.mqlreadmulti(query_slice):
                    if query_result:
                        if isinstance(query_result, list):
                            for result in query_result:
                                if not result['id'] in ids:
                                    ids.append(result['id'])
                                    results.append(result)
                        else:
                            if not query_result['id'] in ids:
                                ids.append(query_result['id'])
                                results.append(query_result)                        
            except MetawebError, detail:
                self._log.error('MQL read error %s on query: %r' % (detail, query_slice))
        return results
    
    def fetch_blurbs(self, session, guids):
        blurbs = []
        for guid in guids:
            if guid:
                try:
                    blurb = session.trans(guid)
                    blurbs.append(codecs.decode(blurb,'utf-8'))
                except MetawebError, detail:
                    self._log.error('Failed to fetch blurb for guid %s - %r' % (guid, detail) )
                    blurbs.append('**Error fetching %s' % guid)
            else:
                blurbs.append('')
        return blurbs

    def score(self, results, types, blurbs):
        scores = []
        for r,blurb in zip(results,blurbs):
            score = 0
            if self._birth_date:
                dob = r['/people/person/date_of_birth']
                if dob:
                    if self._birth_date == dob[:len(self._birth_date)]:
                        if len(self._birth_date) > 4:
                            score += 2.5
                        else:
                            score += 1
                    else:
                        score += -1
                else:
#                    print codecs.encode(blurb[:60], 'utf-8')
                    if blurb[:120].find(self._birth_date) > 0:
                        score += 1
            if self._death_date:
                dod = r['/people/deceased_person/date_of_death']
                if dod:
                    if self._death_date == dod[:len(self._death_date)]:
                        if len(self._death_date) > 4:
                            score += 3
                        else:
                            score += 1
                    else:
                        score += -1
                elif blurb[:120].find(self._death_date) > 0:
                    score += 1

            for t in types:
                if t in r['type']:
                    score += 1
            
            # Look for given & nick names in name, alias, or blurb
            names = list(self._given_names)
            if self._nickname:
                names.append(self._nickname)
            for n in names:
                if r['name'] and r['name'].find(n) >= 0:
                    score += 0.5
                elif r['/common/topic/alias']:
                    for a in r['/common/topic/alias']:
                        if a.find(n) >= 0:
                            score += 0.5
                else:
                    if blurb[:60].find(n) >= 0:
                        score += 0.5
                                    
            b = self.normalize_quotes(blurb[:60])
            if b.find(self.format_full_name()) >= 0:
                score += 2 # big bonus!

            scores.append(score)
        return scores
    
    def mean(self, values):
        if not values or len(values) == 0:
            return 0
        total = 0
        for v in values:
            total += v
        return total / len(values)
    
    def stddev(self, values):
        if not values or len(values) == 0:
            return 0
        mean = self.mean(values)
        squared_diffs = 0
        for v in values:
            x = v - mean
            squared_diffs += (x * x)
        return sqrt(squared_diffs/len(values))

    def score_search(self, results):
        id = None
        score = 0
        name = None
        print '    Search found %d results for %s - above threshold:' % (len(results), self.format_name_with_dates().encode('utf-8','ignore'))
        for r in results:
            s = r['relevance:score']
            if s > SEARCH_RELEVANCE_THRESHOLD and s > score:
                print '       search result %f %s %s' % (r['relevance:score'], r['id'], r['name'].encode('utf-8','ignore'))
                score = r['relevance:score']
                id = r['id']
                name = r['name']

        return score, id, name        

                                                     
    def resolve(self, session, types=[]):
        if self._id:
            return 1
        
        types = ['/people/person', '/government/politician']
        if self._death_date:
            types.append('/people/deceased_person')
        dob = ''
        if self._birth_date:
            dob = self._birth_date
        search_string = (self.format_full_name() + " " + dob).encode('utf8')
        search_results = search(search_string, types)
        search_score, search_id, search_name = self.score_search(search_results)
        if not search_id:
            # Search only indexes anchor text, so if birth year wasn't in an
            # anchor, it can cause search to return no results - try again with it
            search_string = self.format_full_name().encode('utf8')
            search_results = search(search_string, types)
            search_score, search_id, search_name = self.score_search(search_results)            

        all_names = self.format_all_names()
        removes = []
        for n in all_names:
            if len(n.strip().split()) == 1:
                removes.append(n) # don't search on one word names
        for n in removes:
            all_names.remove(n)
        if not self._name_suffix:
            full_name = self.format_full_name()
            first_mi_name = self.format_name(["first", "mi"])
            for s in [', Sr.',', Jr.',' Sr.',' Jr.',' Sr',' Jr',' III',' IV']:
                all_names.add(full_name + s)
                if first_mi_name != full_name:
                    all_names.add(first_mi_name + s)

#        query = [self.name_query(n) for n in all_names]
        query = self.name_query([n for n in all_names])
        results = self.query(session, [query]) 

        if search_id:
            search_result_found = False
            for r in results:
                if r['id'] == search_id:
                    search_result_found = True
                    break
            if not search_result_found:
                print '***First search result (%s) not in our result set, adding it' % search_id
                results.extend(self.query(session, [self.decorate_query({'id':search_id})] ))

        blurbs = self.fetch_blurbs(session, [(r['/common/topic/article'][0]['id'] if r['/common/topic/article'] else None) for r in results  ])    

        if len(results) == 1:
            score = self.score(results, types, blurbs)
            id = results[0]['id']
            if search_id:
                if id != search_id:
                    print ('*WARNING - Search result (%f %s %s) does not match our unique (%f %s) for %s' % (search_score, search_id, search_name, score[0], id, self.format_name_with_dates())).encode('utf8')
#                    return 0
            else:
                print ('*WARNING - Search failed - our algorithm found unique (%f %s) for %s' % (score[0], id, self.format_name_with_dates())).encode('utf8')
            if score > SCORE_THRESHOLD:
                self._id = id
                return 1
            else:
                print ("**Got unique match with low score %f %s for %s" % (score, id, self.format_name_with_dates())).encode('utf8')
                return 0
        else:
            scores = self.score(results, types, blurbs)
            max_score = second_score = 0
            result = None
            for i in range(0,len(scores)):
                r = results[i]
                n = ''
                if r['name']:
                    n = codecs.encode(r['name'], 'utf-8')
                print '   ', scores[i], n, r['/people/person/date_of_birth'],r['id']
                if scores[i] >= max_score:
                    second_score = max_score
                    max_score = scores[i]
                    result = r
                elif scores[i] >= second_score:
                    second_score = scores[i]
                    
            # Compute mean and std deviation without our top score (hopefully it's an outlier)
            mean = stddev = 0.0
            if scores:
                if max_score in scores:
                    scores.remove(max_score)
                mean = self.mean(scores)
                stddev = self.stddev(scores)

            # If top score is more than one std dev from mean, accept the guess
            if result:
                threshold = max(0.5, stddev)                
                # if max_score - mean > threshold: 
                if max_score - second_score > threshold: # Used to check against mean, but let's be more conservative 
                    if search_id and result['id'] != search_id:
                        print ('*WARNING - Search result (%f %s %s) and our algorithm (%s) do not match for %s' % (search_score, search_id, search_name, result['id'], self.format_name_with_dates())).encode('utf8')
#                    else:
                    self._id = result['id']
                    print '       Selected from %d ' % len(results), 'high: %.1f' % max_score, 'second: %.1f' % second_score, 'mean: %.2f ' % mean, 'stddev: %.3f ' % stddev, result['id'], ' for ', self.format_name_with_dates().encode('utf-8', 'ignore')
                    return 1
                else:
                    id = result['id']
                    print '*ERROR - No score above threshold.  Best score: %.1f ' % max_score, 'second: %.1f' % second_score, 'mean: %.2f ' % mean, 'stddev: %.3f ' % stddev, id, ' for ', self.format_name_with_dates().encode('utf8')

#            print results
        
        return len(results)
    
def main():
    names = [['Clarence J. Brown, Jr.', '1927-06-18'],
             ['William Venroe Chappell', '1922'],
#             ['William Vollie "Bill" Alexander','1934'],
#             ['James Allison', '1772'],
#             ['Henry Brush', '1778'],       
#             ['Judah Philip Benjamin', '1811'],
#             ['James Alexander', '1789'],
#             ['James Lusk Alcorn', '1816'],
#             ['James Franklin Aldrich', '1853'],
#             ['Thomas Adams', '1730'],
#             ['George Everett Adams', '1840'],
#             ['Charles Francis Adams', '1807'],
#             ['Robert Adams', '1849'],
#             'Aníbal Acevedo-Vilá',
#             'William Czar Bradley',
#             'Hazel Hempel Abel',
#             'Hazel Abel',
#             'James "Whitey" Bulger',
#             'Brockman "Brock" ADAMS',
#             'W. Todd Akin',
#             'Charles Francis ADAMS',
#             'George BAER',
             ]
    session = HTTPMetawebSession('www.sandbox-freebase.com')
    for n in names:
        p = FbPerson()
        if isinstance(n, str):
            p.parse(n)
        else:
            p.parse(n[0])
            p._birth_date = n[1]
        found = p.resolve(session)
        print n, '-', p.format_full_name(), found, repr(p._id)
    pass

if __name__ == '__main__':
    from freebase.api import HTTPMetawebSession, MetawebError
    main()