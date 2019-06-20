from py2neo import Node, Relationship
from py2neo import Graph, Schema
import csv

graph = Graph("bolt://localhost:7687", auth=('neo4j', '123456'))

all_nodes = graph.run("match (n)-[r]->() with n, count(r) as rel_cnt where rel_cnt > 50 return n")

all_relationships = graph.run('MATCH (a)-[b]->(c) RETURN a, type(b),c ')

id_list = []

with open('output/event_nodes.csv', mode='w', encoding='utf-8', newline='') as event_nodes, \
        open('output/person_nodes.csv', mode='w', encoding='utf-8', newline='') as person_nodes, \
        open('output/venue_nodes.csv', mode='w', encoding='utf-8', newline='') as venue_nodes, \
        open('output/non_scraped.csv', mode='w', encoding='utf-8', newline='') as non_scraped:

    person_fieldnames = ['Id', 'Label', 'Weight', 'Category', 'scrape_time']

    event_fieldnames = ['Id', 'Label', 'Weight', 'Category', 'scrape_time']

    venue_fieldnames = ['Id', 'Label', 'Weight', 'Category', 'city', 'latitude', 'longitude', 'scrape_time']

    non_scraped_fieldnames = ['url']

    event_writer = csv.DictWriter(event_nodes, fieldnames=event_fieldnames)
    person_writer = csv.DictWriter(person_nodes, fieldnames=person_fieldnames)
    venue_writer = csv.DictWriter(venue_nodes, fieldnames=venue_fieldnames)
    url_writer = csv.DictWriter(non_scraped, fieldnames=non_scraped_fieldnames)

    event_writer.writeheader()
    person_writer.writeheader()
    venue_writer.writeheader()
    url_writer.writeheader()

    event_writer = csv.writer(event_nodes)
    person_writer = csv.writer(person_nodes)
    venue_writer = csv.writer(venue_nodes)
    url_writer = csv.writer(non_scraped)

    for node in all_nodes:
        # do something with node here

        node_dict = dict(node[0])  # node dict
        node_label = list(node[0].labels)[0]
        node_len = len(node_dict)
        node_id = node[0].identity

        # print(node)
        # print(type(node[0]))
        print(node_id)

        if node_len == 1:

            url = node_dict['url']
            row_to_write = [url]
            url_writer.writerow(row_to_write)

        elif node_label == 'event':
            id_list.append(node_id)
            # url = node_dict['url']
            # row_to_write = [url]
            # url_writer.writerow(row_to_write)

            # print(node_dict)

            label = node_dict['name']
            weight = 1
            category = node_label
            scrape_time = node_dict['scrape_time']

            row_to_write = [node_id, label, weight, category, scrape_time]
            event_writer.writerow(row_to_write)

        elif node_label == 'person':
            label = node_dict['name']
            weight = 1
            category = node_label
            scrape_time = node_dict['scrape_time']

            id_list.append(node_id)
            row_to_write = [node_id, label, weight, category, scrape_time]
            person_writer.writerow(row_to_write)

        elif node_label == 'venue':
            id_list.append(node_id)
            # print('it was a venue')
            label = node_dict['name']
            weight = 1
            category = node_label
            scrape_time = node_dict['scrape_time']
            city = node_dict['city']
            latitude = node_dict['latitude']
            longitude = node_dict['longitude']

            row_to_write = [node_id, label, weight, category, city, latitude, longitude, scrape_time]
            venue_writer.writerow(row_to_write)
'''
x = 0
with open('output/edges.csv', mode='w', encoding='utf-8', newline='') as edges:

    edge_fieldnames = ['Source', 'Target', 'Label', 'Weight']
    edge_writer = csv.DictWriter(edges, fieldnames=edge_fieldnames)
    edge_writer.writeheader()

    edge_writer = csv.writer(edges)

    for relationship in all_relationships:
        print(x)
        x += 1
        source_id = relationship[0].identity
        rel_type = relationship[1]
        target_id = relationship[2].identity

        if source_id in id_list or target_id in id_list:
            row_to_write = [source_id, target_id, rel_type, 1]
            edge_writer.writerow(row_to_write)
'''
