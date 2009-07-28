#
# A prototype relevance reconciler
#
# It understands data which consists of a list of lists of property dictionaries
#
# Usage:
#
# execfile( 'relrec.pl' )
# reconcile_objects( '/film/film', e )

import simplejson
import urllib
from freebase.api import HTTPMetawebSession, MetawebError

ect = {
	'/type/object/name' : '/type/text' }

def ect_for_property( property ):
	if property not in ect:
		query = {
			'id' : property,
			'/type/property/expected_type' : None, }
		mss = HTTPMetawebSession( 'sandbox.freebase.com' )
		result = mss.mqlread( query )
		ect[property] = result['/type/property/expected_type']
	return ect[property]


def constraint_for( property, ect, guids ):
	if property == '/type/object/name':
		if guids and len( guids ) > 0:
			return { "guid|=" : guids, 'name' : None, "id" : None }
		else:
			return { "name" : None }
	else:
		if guids and len( guids ) > 0:
			return { property : [{ "guid|=" : guids, 'name' : None, "id" : None }] }
		else:
			return { property : [{ 'type' : ect, 'optional' : True, 'name' : None }] }


def query_for( type, properties ):
	q = { "id" : None, "type" : type }
	for p in properties:
		guids = map( lambda( x ): x['guid'], p['results'] )
		q.update( constraint_for( p['property'], p['type'], guids ) )
	return [q]


srv = "http://sandbox.freebase.com/api/service/newsearch?"
#srv = "http://index02.sandbox.sjc1.metaweb.com:8118/api/service/search?"


def reconcile_objects( ty, data ):
	failed_matches = []
	for match in data:
		n_rel_matches = 0
		properties = [];
		object_id = None
		for property in match:
			p = property['property'].strip()
			n = property['name'].strip()
			id = property['id'].strip()
			if p == '/type/object/name':
				object_id = id
				t = ty
			else:
				t = ect_for_property( p )
			args = {
				"type" : t,
				"limit" : 20,
				"query" : property['name'].strip(),
				"mql_output" : simplejson.dumps( [{ "name" : None, "guid" : None }] ), }
			url = srv + urllib.urlencode( args )
			j = simplejson.load( urllib.urlopen( url ) )
			if not j['code'] == '/api/status/ok':
				print "ERROR: " + p + "\n"
				print simplejson.dumps( j ) + "\n"
				continue
			if len( j['result'] ) > 0:
				n_rel_matches += 1
			properties.append(
				{
					'property' : p,
					'name' : n,
					'type' : t,
					'id' : id,
					'results' : j['result'] } )
		if n_rel_matches > 1:
			q = query_for( ty, properties )
			mss = HTTPMetawebSession( 'sandbox.freebase.com' )
			result = mss.mqlread( q )
			print simplejson.dumps( properties, indent=2 ) + "\n"
			print simplejson.dumps( q, indent = 2 ) + "\n"
			if len( result ) > 0:
				print simplejson.dumps( result, indent = 2 ) + "\n"
			else:
				print "NO MATCH: " + object_id + "\n"
				failed_matches.append( object_id )
		else:
			print "Zero or one Relevance matches\n"
		print "\n\n==========\n\n"
	print failed_matches


a = eval( file ("a-business_company.txt").read() )
b = eval( file ("b-business_company.txt").read() )
c = eval( file ("c-religion_religion.txt").read() )
d = eval( file ("d-baseball_baseball_player.txt").read() )
e = eval( file ("e-film_film.txt").read() )

#reconcile_objects( '/business/company', a )
#reconcile_objects( '/business/company', b )
#reconcile_objects( '/religion/religion', c )
#reconcile_objects( '/baseball/baseball_player', d )
#reconcile_objects( '/film/film', e )
