from py2neo import Node, Relationship
from py2neo import Graph, Schema


class ArtistPipeline(object):  # definition of the pipeline class

    person_list = []
    venue_list = []
    event_list = []

    def __init__(self):
        pass

    def process_item(self, item, spider):  # item process definition

        transaction = self.graph.begin()  # create a graph object
        item_type = item.get('category')  # get the category of the object

        if item_type == 'person':  # if the object is a person
            person = Node(item.get('category'),  # fill the fields with appropriate values
                          url=item.get('url'),
                          name=item.get('name'),
                          bio=item.get('bio'),
                          scrape_time=item.get('scrape_time'))
            person.__primarylabel__ = 'person'
            person.__primarykey__ = 'url'
            sub_graph = person

            for venues in item.get('venue_list'):

                venue = self.graph.evaluate('match (x:venue {url:{param}}) return x', param=venues)
                if venue is not None and len(venue) > 1:
                    pass
                else:
                    venue = Node('venue', url=venues)

                venue.__primarylabel__ = 'venue'
                venue.__primarykey__ = 'url'
                sub_graph = sub_graph | venue
                sub_graph = sub_graph | Relationship(person, 'has an exhibition in', venue)

            for events in item.get('event_list'):
                event = self.graph.evaluate('match (x:event {url:{param}}) return x', param=events)
                if event is not None and len(event) > 1:
                    pass
                else:
                    event = Node('event', url=events)

                event.__primarylabel__ = 'event'
                event.__primarykey__ = 'url'
                sub_graph = sub_graph | event
                sub_graph = sub_graph | Relationship(person, 'participated in', event)

            transaction.merge(sub_graph)
            print('person item')

        elif item_type == 'event':
            event = Node(item.get('category'),
                         url=item.get('url'),
                         name=item.get('name'),
                         start_date=item.get('start_date'),
                         end_date=item.get('end_date'),
                         press_release=item.get('press_release'),
                         scrape_time=item.get('scrape_time'))
            event.__primarylabel__ = 'event'
            event.__primarykey__ = 'url'
            sub_graph = event
            venue_url = item.get('venue')

            venue = self.graph.evaluate('match (x:venue {url:{param}}) return x', param=venue_url)
            if venue is not None and len(venue) > 1:
                pass
            else:
                venue = Node('venue', url=item.get('venue'))

            venue.__primarylabel__ = 'venue'
            venue.__primarykey__ = 'url'

            sub_graph = sub_graph | venue
            sub_graph = sub_graph | Relationship(event, 'took place in', venue)

            for participants in item.get('participant_list'):

                participant = self.graph.evaluate('match (x:person {url:{param}}) return x', param=participants)
                if participant is not None and len(participant) > 1:
                    pass
                else:
                    participant = Node('person', url=participants)

                participant.__primarylabel__ = 'person'
                participant.__primarykey__ = 'url'
                sub_graph = sub_graph | participant
                sub_graph = sub_graph | Relationship(participant, 'participated in', event)

            transaction.merge(sub_graph)
            print('event item')

        elif item_type == 'venue':
            venue = Node(item.get('category'),
                         url=item.get('url'),
                         name=item.get('name'),
                         city=item.get('city'),
                         latitude=item.get('latitude'),
                         longitude=item.get('longitude'),
                         street_address=item.get('street_address'),
                         website=item.get('website'),
                         email=item.get('email'),
                         scrape_time=item.get('scrape_time'))
            venue.__primarylabel__ = 'venue'
            venue.__primarykey__ = 'url'
            sub_graph = venue

            for venues in item.get('connected_venue_list'):
                connected_venue = self.graph.evaluate('match (x:venue {url:{param}}) return x', param=venues)
                if connected_venue is not None and len(connected_venue) > 1:
                    pass
                else:
                    connected_venue = Node('venue', url=venues)

                connected_venue.__primarylabel__ = 'venue'
                connected_venue.__primarykey__ = 'url'
                sub_graph = sub_graph | connected_venue
                sub_graph = sub_graph | Relationship(connected_venue, 'is cooperating with', venue)

            for events in item.get('event_list'):
                event = self.graph.evaluate('match (x:event {url:{param}}) return x', param=events)
                if event is not None and len(event) > 1:
                    pass
                else:
                    event = Node('event', url=events)

                event.__primarylabel__ = 'event'
                event.__primarykey__ = 'url'
                sub_graph = sub_graph | event
                sub_graph = sub_graph | Relationship(event, 'took place in', venue)

            for artists in item.get('artist_list'):
                artist = self.graph.evaluate('match (x:person {url:{param}}) return x', param=artists)
                if artist is not None and len(artist) > 1:
                    pass
                else:
                    artist = Node('person', url=artists)

                artist.__primarylabel__ = 'person'
                artist.__primarykey__ = 'url'
                sub_graph = sub_graph | artist
                sub_graph = sub_graph | Relationship(artist, 'has an exhibition in', venue)

            transaction.merge(sub_graph)
            print('venue item')

        else:
            print('invalid item')

        transaction.commit()
        return item

    def open_spider(self, spider):
        self.graph = Graph("bolt://localhost:7687", auth=('neo4j', '123456'))  # connect to graph
        self.graph.schema.create_uniqueness_constraint("person", "url")
        self.graph.schema.create_uniqueness_constraint("venue", "url")
        self.graph.schema.create_uniqueness_constraint("event", "url")

    def close_spider(self, spider):
        pass
