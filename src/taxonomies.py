'''
Created on September 8, 2009

Find all properties which are reflexive and expect their own type as a target.

@author: Tom Morris <tfmorris@gmail.com>
'''

from freebase.api import HTTPMetawebSession, MetawebError

def get_properties(session):
    query = [{"type": "/type/property", 
              "id": None, 
              "name": None, 
              "schema": None, 
              "expected_type": None, 
              "unique": None, 
              "reverse_property": {'name': None, 'id': None, '/type/property/unique': None}, #'optional': False
#              "master_property": {'name': None, 'id': None, '/type/property/unique': None},
              "limit": 100}]
    return session.mqlreaditer(query)

def get_opposite(prop):
    opposite_name = ''
    opposite_id = None
#   if prop.master_property:
#       opposite_name = prop.master_property.name
#       opposite_id = prop.master_property.id
#   elif r.reverse_property:
    if prop.reverse_property:
        opposite_name = prop.reverse_property.name
        opposite_id = prop.reverse_property.id
    return opposite_id, opposite_name

def check_consistency(prop):
    if not prop.id.startswith(prop.schema):
        print ' * Schema is not parent ' + prop.schema + ' ' + prop.id

def get_counts(session, r):
    # Skip problematic entries
    if r.schema.find('$') >= 0 or r.id.find('/user/bio2rdf/public/bm') >= 0:
        return -1, -1
    else:
        # The count from last night's tallybot is good enough (estimate-count)
        q = {'type': r.schema, 'return': 'estimate-count'}
        type_count = session.mqlread(q)
        q.update({r.id: [{'id': None, 'optional': False}]})
#       del q[r.id]
#       q.update({r.reverse_property.id,[{'id': None, 'optional': False}]})
        property_count = session.mqlread(q)
    return type_count, property_count
                
def main():
    session = HTTPMetawebSession('www.freebase.com')
    matches = 0
    total = 0
    print '\t'.join(['Type ID', 'Instances', '# of Subgraphs',
                     'Max size of subgraph', 'Max subgraph ID',
                     'Avg size of Subgraph',
                     '# of cycles'])
    for r in get_properties(session):
        total += 1
        # Only look at properties where expected type is same as owning type
        if r.schema and r.schema == r.expected_type:
            matches += 1
            check_consistency(r)
            opposite_id, opposite_name = get_opposite(r)
            if not opposite_id: # Skip properties with no reverse
                continue
            type_count, property_count = get_counts(session, r)

            # Skip user types, bases, and low usage types
            if property_count > 2 and r.id[:5] != '/user' and r.id[:5] != '/base':
#                try:
#                    print '\t'.join([str(matches), str(type_count), str(property_count), 
#                                     r.name, r.id, str(r.unique), 
#                                     opposite_name, str(opposite_id), str(r.reverse_property['/type/property/unique'])])
#                except:
#                    print '** ' + str(matches) + ' ' + repr(r)

                # For debugging skip really big sets
                if property_count < 700 and type_count < 5000:
                    #print '%s - property count %d, type count %d' % (r.id, property_count, type_count)
                    traverse_tree(session, r.schema, [r.id, opposite_id])
                else:
                    print '\tSkipping %s - property count %d, type count %d' % (r.id, property_count, type_count)

    print '\n\nFound ' + str(matches) + ' in ' + str(total) + ' total properties.'

def visit(id, previous, ids, props, seen, cyclic):
    if id in seen and seen[id] > 1:
        return cyclic
    seen[id] = 1
    if id in ids:
        item = ids[id]
    else:
        # If it's not in our collection, it probably means it doesn't have the right type
        return cyclic
    links = []
    for p in props:
        links.extend([l.id for l in item[p]])
    for l in links:
        if l != previous:
            if l in seen:
#                print 'Cycle detected ' + l + ' ' + id
                cyclic = True
            else:
                cyclic |= visit(l, id, ids, props, seen, cyclic)
    return cyclic
            
def traverse_tree(session, type_id, props):
    q = {'type': type_id,
         'id' : None}
    for p in props:
        q.update({p:[{'id': None, 
                      'optional' : True}]})

    ids = dict([(r.id, r) for r in session.mqlreaditer([q])])

    seen = {}
    todo = {}
    subgraphs = []
    subgraph_count = 0
    subgraph_max_size = 0
    subgraph_max_id = ''
    cycle_count = 0
        
    for i in ids:
        if not i in seen:
            subgraph = {}
            cyclic = visit(i, None, ids, props, subgraph, False)
            subgraphs.append((i,subgraph))
            seen.update(subgraph)
            subgraph_count += 1
            if (len(subgraph) > subgraph_max_size):
                subgraph_max_size = len(subgraph)
                subgraph_max_id = i
            if cyclic:
                cycle_count += 1
#            print 'Subgraph id ' + i + ' size ' + str(len(subgraph))
            
    print '\t'.join([type_id, str(len(ids)), str(subgraph_count),
                     str(subgraph_max_size), subgraph_max_id,
                     str(len(ids) * 1.0 /subgraph_count),
                     str(cycle_count)])
        
def get_root(id, ids, prop):
    if not id in ids:
        print "Id not found "+ id
        return None
    item = ids[id]
    if not item[prop]:
        return id
    if len(item[prop])>1:
        return None
    return get_root(item[prop][0].id, ids, prop)


def traverse_subtree(list, map, props):
    item = list.pop()
    if item.id in map:
        return
    map[item.id] = item
    for p in props:
        pass
        
        
def test():
    session = HTTPMetawebSession('www.freebase.com')
    props = ['/music/instrument/family', '/music/instrument/variation']
    traverse_tree(session, '/music/instrument', props)
    
if __name__ == '__main__':
    main()