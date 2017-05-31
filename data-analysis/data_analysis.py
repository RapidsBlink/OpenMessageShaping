with open('log.txt', 'r') as ifs:
    lines = filter(lambda line: 'main' in line, ifs.readlines())
    record_list = set(map(lambda line: line.split('main:')[1], lines))
    record_pair_list = map(lambda record: record.strip().split(','), record_list)

    queue_list = filter(lambda ele: 'QUEUE' in ele[0], record_pair_list)
    topic_list = filter(lambda ele: 'TOPIC' in ele[0], record_pair_list)
    for queue_pair in sorted(queue_list, key=lambda pair: int(pair[0].split('_')[1])):
        print queue_pair

    print

    for topic_pair in sorted(topic_list, key=lambda pair: int(pair[0].split('_')[1])):
        print topic_pair

    key_value_list = map(lambda lst: (lst[0], int(lst[1])), record_pair_list)
    file_size_list = map(lambda my_pair: my_pair[1], key_value_list)

    total_size = sum(file_size_list)
    print total_size
    print float(total_size) / 1024 / 1024 / 1024

    print 4.66584196314
