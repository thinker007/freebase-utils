'''
Created on September 8, 2009

Find all properties which are reflexive and expect their own type as a target.

@author: Tom Morris <tfmorris@gmail.com>
'''

from freebase.api import HTTPMetawebSession, MetawebError

PROPERTY_MAX = 250 # max number of property instances to consider
TYPE_MAX = 10000 # Max number of type instances to consider

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
        q.update({r.id : [{'id': None, 'optional': False}]})
        forward_property_count = session.mqlread(q)
        del q[r.id]
        q.update({r.reverse_property.id : [{'id': None, 'optional': False}]})
        reverse_property_count = session.mqlread(q)
    return type_count, forward_property_count, reverse_property_count

def which_way_up(prop):
    # return 0 for master property navigates towards root, 1 for reverse, -1 for neither
    if prop.unique and prop.reverse_property.unique:
        # Both unique probably means sequence next/previous
        return -1
    if prop.unique and not prop.reverse_property.unique:
        return 0
    if not prop.unique and prop.reverse_property.unique:
        return 1

    # Hmmm, they're both non-unique
    # let's try computing the fanout degree in each direction
    
    # Alternatively, traverse in each direction looking for one direction which
    # never has more than one out edge
    
def out_degree(prop, nodes):
    edge_count = 0
    max_edges = 0
    node_count = 0
    
    next = prop
    while next:
        oe = edges()
        max_edges = max(max_edges, oe)
        edge_count += oe
        node_count += 1
        next = next[prop][0].id

def main():
    session = HTTPMetawebSession('www.freebase.com')
    matches = 0
    total = 0
#    print '\t'.join(['Type ID', 'Instances', '# of Subgraphs',
#                     'Max size of subgraph', 'Max subgraph ID',
#                     'Avg size of Subgraph',
#                     '# of cycles'])
    for r in get_properties(session):
        total += 1
        # Only look at properties where expected type is same as owning type
        if r.schema and r.schema == r.expected_type:
            check_consistency(r)
            opposite_id, opposite_name = get_opposite(r)
            # Skip properties with no reverse
            if not opposite_id:
                continue
            # Skip bases and user types
            if r.id[:5] == '/user' or r.id[:5] == '/base':
                continue
            matches += 1

            type_count, for_property_count, rev_property_count = get_counts(session, r)

            # Skip low usage types
            property_count = for_property_count + rev_property_count
            if property_count > 2: 
                try:
                    print '\t'.join([str(matches), str(type_count), 
                                     str(for_property_count), str(rev_property_count), 
                                     r.name, r.id, str(r.unique), 
                                     opposite_name, str(opposite_id), str(r.reverse_property['/type/property/unique'])])
                except:
                    print '** ' + str(matches) + ' ' + repr(r)

                # For debugging skip really big sets
                if property_count < PROPERTY_MAX and type_count < TYPE_MAX:
#                    print '%s - property count %d, type count %d' % (r.id, property_count, type_count)
                    traverse_tree(session, r.schema, [r.id, opposite_id])
                else:
                    print '\tSkipping %s - property count %d, type count %d' % (r.id, property_count, type_count)

    print '\n\nFound ' + str(matches) + ' in ' + str(total) + ' total properties.'

def visit(id, previous, items, props, seen, cyclic):
    if id in seen and seen[id] > 1:
        return True
    seen[id] = 1
    if id in items:
        item = items[id]
    else:
        # If it's not in our collection, it probably means it doesn't have the right type
        return cyclic
    links = []
    for p in props:
        # TODO Track links separately for each direction/property
        links.extend([l.id for l in item[p]])
    for l in links:
        if l != previous:
            if l in seen and seen[l] > 1:
                print 'Cycle detected ' + l + ' ' + id
                cyclic = True
            else:
                cyclic |= visit(l, id, items, props, seen, cyclic)
                # Mark our down link as fully visited
                seen[l] = 2
    return cyclic
            
def traverse_tree(session, type_id, props):
    print 'Traversing type %s with props %s, %s' %(type_id, props[0], props[1])
    
    # Items with both property values
    q = {'type': type_id,
         'id' : None,
         props[0] : [{'id' :None}],
         props[1] : [{'id' :None}]
         }
    items = dict([(r.id, r) for r in session.mqlreaditer([q])])
    both = len(items)

    # Only the first property value
    q[props[0]] = [{'id' : None}]
    q[props[1]] = [{'id' : None, 'optional' : 'forbidden'}]
    more = dict([(r.id, r) for r in session.mqlreaditer([q])])
    first = len(more)
    items.update(more)
    
    # Only the second property value
    q[props[0]] = [{'id' : None, 'optional' : 'forbidden'}]
    q[props[1]] = [{'id' : None}]
    more = dict([(r.id, r) for r in session.mqlreaditer([q])])
    second = len(more)
    items.update(more)

    print 'Total items = %d (%d, %d, %d)' % (len(items), both, first, second)
    
    seen = {}
    todo = {}
    subgraphs = []
    subgraph_count = 0
    subgraph_max_size = 0
    subgraph_max_id = ''
    cycle_count = 0
        
    for id in items:
        if not id in seen:
            subgraph = {}
            cyclic = visit(id, None, items, props, subgraph, False)
            subgraphs.append((id,subgraph))
            seen.update(subgraph)
            subgraph_count += 1
            if (len(subgraph) > subgraph_max_size):
                subgraph_max_size = len(subgraph)
                subgraph_max_id = id
            if cyclic:
                cycle_count += 1
#            print 'Subgraph id ' + id + ' size ' + str(len(subgraph))
            
    print '\t'.join([type_id, str(len(items)), str(subgraph_count),
                     str(subgraph_max_size), subgraph_max_id,
                     str(len(items) * 1.0 /subgraph_count),
                     str(cycle_count)])
        
def get_root(id, items, prop):
    if not id in items:
        print "Id not found "+ id
        return None
    item = items[id]
    if not item[prop]:
        return id
    if len(item[prop])>1:
        return None
    return get_root(item[prop][0].id, items, prop)


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
#    test()