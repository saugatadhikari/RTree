from rtree import index


def read_doc_file(file_name: str) -> dict:
    """ Read doc file with format obj_id, label_id"""
    obj_dict = {}
    with open(file_name, 'rb') as f:
        for line in f:
            line = line.decode("utf-8").strip("\n")
            obj, label = line.split(",")
            obj_dict[int(obj)] = int(label)
    
    return obj_dict


def read_loc_file(file_name: str) -> list:
    """ Read loc file with format obj_id, x, y"""
    loc_list = []
    with open(file_name, 'rb') as f:
        for line in f:
            line = line.decode('utf-8').strip('\n')
            obj, x, y = line.split(',')
            loc_list.append((float(x), float(y)))
    return loc_list


# Function required to bulk load the random points into the index
# Looping over and calling insert is orders of magnitude slower than this method
def generator_function(points):
    for i, coord in enumerate(points):
        x, y = coord
        yield (i, (x, y, x, y), coord)


def create_rtree_single(points, insert_objects=False):
    # create new r-tree
    tree = index.Index()
    # add list of points to r-tree
    print("Creating rtree takes some time...")
    for i, lat_lon in enumerate(points):
        if not insert_objects:
            tree.insert(i, lat_lon)
        else:
            tree.insert(i, lat_lon, obj=lat_lon)
    print("Done!")
    return tree

def create_rtree(points, insert_objects=False):
    # add list of points to r-tree
    print("Creating rtree takes some time...")
    
    # create new r-tree
    tree = index.Index(generator_function(points))

    print("Done!")
    return tree

# very rough estimate !!!
def get_bounding_box(lat, lon, d):
    """
    Using a point(lat, lon), find the minimum and maximum corners that form a box within 'd' distance (in KM). 
    """
    min_lat = lat - d
    max_lat = lat + d
    min_lon = lon - d
    max_lon = lon + d
    return min_lat, min_lon, max_lat, max_lon


def get_nearby(r_tree, point, r, return_objects=False):
    """
    Get all candidates within [r] KM around [point] using [r_tree]
    """
    min_lat, min_lon, max_lat, max_lon = get_bounding_box(point[0], point[1], r)
    if return_objects: 
        return list(r_tree.intersection((min_lat, min_lon, max_lat, max_lon ), objects=True))
    else:
        return list(r_tree.intersection((min_lat, min_lon, max_lat, max_lon )))


def get_nearest(r_tree, point, count=1, return_objects=False):
    """
    return [count] nearest neighbor/s to [point]
    """
    # the bounding box is the point itself
    min_lat, min_lon, max_lat, max_lon = point[0],point[1],point[0],point[1]
    if return_objects:
        return list(r_tree.nearest((min_lat, min_lon, max_lat, max_lon ), count, objects=True))
    else:
        return list(r_tree.nearest((min_lat, min_lon, max_lat, max_lon ), count))


def pipeline(loc_file: str, doc_file: str, radius: int, output_file: str) -> None:
    """
        Read location file and doc file with objects and location(x, y) and converts to required graph format
    """
    obj_dict = read_doc_file(doc_file)
    loc_list = read_loc_file(loc_file)

    r_tree = create_rtree(loc_list)

    num_vertices = len(loc_list)
    num_edges = 0

    fp = open(output_file, 'w')

    vertex_string = ""
    edge_string = ""

    for i, query_point in enumerate(loc_list):
        obj_id = i + 1 # obj id
        label_id = obj_dict[obj_id] # label id
        
        nearby_points = get_nearby(r_tree, query_point, radius, return_objects=False)
        degree = len(nearby_points) - 1 # degree of vertex, minus 1 because it gives self loop as well
        
        vertex_string += f"v {obj_id} {label_id} {degree}\n"
        
        for nearby_point in nearby_points:
            nearby_obj_id = nearby_point + 1
            if obj_id < nearby_obj_id:
                edge_string += f"e {obj_id} {nearby_obj_id} 1\n"
                num_edges += 1

    fp.write(f"t {num_vertices} {num_edges}\n")
    fp.write(vertex_string)
    fp.write(edge_string)
    fp.close()

    return


if __name__ == "__main__":
    LOC_FILE = "./syn_loc_small.txt"
    DOC_FILE = "./syn_doc_small.txt"
    RADIUS = 5
    OUTPUT_FILE = "output_graph.txt"

    pipeline(LOC_FILE, DOC_FILE, RADIUS, OUTPUT_FILE)