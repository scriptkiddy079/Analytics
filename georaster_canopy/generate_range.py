def makeTupleList(min_id, max_id, total, num_cores):
    step = total // num_cores
    id_range = []
    start = min_id
    end = min_id + step
    for x in range(num_cores):
        if x != num_cores - 1:
            id_range.append([start, end])
            start = end
            end += step
        else:
            id_range.append([start,  max_id])
    return id_range


def main():
    min_rid = 11020229727
    max_rid = 11015481676123
    count_roads = 18123444
    min_geo = 1
    max_geo = 253762
    count_geos = 247986
    roads_list = makeTupleList(min_rid, max_rid, count_roads, 8)
    # print(roads_list)
    for x in roads_list:
        print(x)
    print(len(roads_list))


if __name__ == '__main__':
    main()
