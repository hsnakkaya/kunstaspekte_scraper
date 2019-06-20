import scrapy
from ..items import PersonItem, VenueItem, EventItem
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup
import datetime
import csv

# scrapy crawl artist -s JOBDIR=crawls/artist-1


class ArtistSpider(scrapy.Spider):
    name = "artist"
    domain = 'https://kunstaspekte.art'

    def start_requests(self):
        # urls = ['https://kunstaspekte.art/event/mediations-biennale-poznan-2010-event',
        #         'https://kunstaspekte.art/event/deuscthland-eine-ausstellung-von-jan-boehmermann-und-btf',
        #         'https://kunstaspekte.art/venue/biennial-of-graphic-arts-ljubljana-venue',
        #         'https://kunstaspekte.art/person/michel-blazy',
        #         'https://kunstaspekte.art/person/index-books-peter-gidal',
        #         'https://kunstaspekte.art/person/shannon-ebner'
        #         ]
        # for url in urls:
        #     print(url)
        #     yield scrapy.Request(url=url, callback=self.parse)

        #  all site
        """

        page_list = {'0', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                     'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'}
        url = 'https://kunstaspekte.art/artists-overview/'
        for char in page_list:
            yield scrapy.Request(url=url + char, callback=self.parse_initial)

        # url = 'https://kunstaspekte.art/artists-overview/'
        # yield scrapy.Request(url=url + 'a', callback=self.parse_initial)

        """

        with open('non_scraped.csv', 'r') as f:
            a = set([row[0] for row in csv.reader(f)])
            for urls in a:
                yield scrapy.Request(url=urls, callback=self.parse, dont_filter=True, priority=1000)
                print('urls')

    def parse_initial(self, response):

        raw = response.body
        soup = BeautifulSoup(raw, 'html.parser')
        url_list = soup.find_all('a')

        for urls in url_list:
            yield scrapy.Request(url=self.domain + urls['href'], callback=self.parse)
            print(self.domain + urls['href'])

    def parse(self, response):

        print(response)
        raw = response.body
        soup = BeautifulSoup(raw, 'html.parser')

        try:
            page_type = soup.find(class_='content-heading').find('h3').get_text(' ', strip=True)
            print(page_type)
        except AttributeError:
            page_type = 0
            print("An exception occurred")

        if page_type == 'artist / curator':
            yield from self.person_parse(response)
            print('person page')
        elif page_type == 'venue':
            yield from self.venue_parse(response)
            print('venue page')
        elif page_type == 'exhibition':
            print('exhibition page')
            yield from self.event_parse(response)
        else:
            print('non valid page')

    def event_parse(self, response):

        print('event scraper')
        event = EventItem()
        # category = scrapy.Field()
        # url = scrapy.Field()
        # name = scrapy.Field()
        # venue = scrapy.Field()
        # start_date = scrapy.Field()
        # end_date = scrapy.Field()
        # press_release = scrapy.Field()
        # participant_list = scrapy.Field()

        category = 'event'

        url = response.url

        raw = response.body
        soup = BeautifulSoup(raw, 'html.parser')

        name = ''
        try:
            name = soup.find('h1').get_text(' ', strip=True)
        except Exception as ex:
            print(ex)

        venue = ''
        try:
            venue = self.domain + soup.find(class_='venue-module').find('h3').find('a')['href']
        except Exception as ex:
            print(ex)

        start_date = ''
        try:
            start_date = soup.find(class_='begins').get_text(' ', strip=True)
        except Exception as ex:
            print(ex)

        end_date = ''
        try:
            end_date = soup.find(class_='ends').get_text(' ', strip=True)
        except Exception as ex:
            print(ex)

        press_release = ''
        try:
            text = soup.find(id='textblock').find_all('p')
            for i in text:
                press_release = press_release + i.get_text('\n', strip=True)
        except Exception as ex:
            print(ex)

        participant_list = []
        try:
            participants = soup.find_all(class_='artist-list')
            for i in participants:
                links = i.find_all('a')
                for j in links:
                    participant_list.append(self.domain + j['href'])
        except Exception as ex:
            print(ex)

        event['category'] = category
        event['url'] = url
        event['name'] = name
        event['venue'] = venue
        event['start_date'] = start_date
        event['end_date'] = end_date
        event['press_release'] = press_release
        event['participant_list'] = participant_list
        event['scrape_time'] = datetime.datetime.now()

        # yield scrapy.Request(url=venue, callback=self.parse)
        #
        # for url in participant_list:
        #     yield scrapy.Request(url=url, callback=self.parse)

        yield event

    def venue_parse(self, response):

        print('venue scraper')
        venue = VenueItem()
        # category = scrapy.Field()
        # url = scrapy.Field()
        # name = scrapy.Field()
        # connected_venue_list = scrapy.Field()
        # event_list = scrapy.Field()
        # artist_list = scrapy.Field()
        # city = scrapy.Field()
        # coordinates = scrapy.Field()
        # street_address = scrapy.Field()
        # website = scrapy.Field()
        # email = scrapy.Field()

        category = 'venue'

        url = response.url

        raw = response.body
        soup = BeautifulSoup(raw, 'html.parser')

        name = ''
        try:
            name = soup.find('h1').get_text(' ', strip=True)
        except Exception as ex:
            print(ex)

        connected_venue_list = []
        try:
            dependencies = soup.find(id='texts').find_all('a')
            for i in dependencies:
                connected_venue_list.append(self.domain + i['href'])
        except Exception as ex:
            print(ex)

        event_list = []
        try:
            exhibition = soup.find_all(class_='exhib-title')
            for links in exhibition:
                event_list.append(self.domain + links['href'])
        except Exception as ex:
            print(ex)

        artist_list = []
        try:
            artists = soup.find(class_='artist-list').find_all('a')
            for links in artists:
                artist_list.append(self.domain + links['href'])
        except Exception as ex:
            print(ex)

        city = ''
        coordinates = ['', '']
        street_address = ''
        website = ''
        mail = ''
        try:
            address = soup.find('div', class_='address')
            city = address.find('p').find('a').get_text(' ', strip=True)
            coordinates = address['data-latlon'].split(',')
            street_address = address.find('p').get_text(' ', strip=True)
            website = address.find(class_='website')['href']
            mail = address.find(class_='mail')['href'].split(':')[1]
        except Exception as ex:
            print(ex)

        venue['category'] = category
        venue['url'] = url
        venue['name'] = name
        venue['connected_venue_list'] = connected_venue_list
        venue['event_list'] = event_list
        venue['artist_list'] = artist_list
        venue['city'] = city
        venue['latitude'] = coordinates[0]
        venue['longitude'] = coordinates[1]
        venue['street_address'] = street_address
        venue['website'] = website
        venue['email'] = mail
        venue['scrape_time'] = datetime.datetime.now()

        # for url in connected_venue_list:
        #     yield scrapy.Request(url=url, callback=self.parse)
        #
        # for url in event_list:
        #     yield scrapy.Request(url=url, callback=self.parse)
        #
        # for url in artist_list:
        #     yield scrapy.Request(url=url, callback=self.parse)

        yield venue

    def person_parse(self, response):

        print('person scraper')
        person = PersonItem()
        # category = scrapy.Field()
        # url = scrapy.Field()
        # name = scrapy.Field()
        # bio = scrapy.Field()
        # venue_list = scrapy.Field()
        # event_list = scrapy.Field()

        category = 'person'

        url = response.url
        print(url)

        raw = response.body
        soup = BeautifulSoup(raw, 'html.parser')

        name = ''
        try:
            name = soup.find('h1').get_text(' ', strip=True)
        except Exception as ex:
            print(ex)

        bio = ''
        try:
            for i in soup.find_all('p'):
                bio = bio + ' ' + i.get_text(' ', strip=True)
        except Exception as ex:
            print(ex)

        venue_list = []
        try:
            collections = soup.find('div', class_='collections').find_all('a')
            for collection in collections:
                venue_list.append(self.domain + collection['href'])
        except Exception as ex:
            print(ex)

        try:
            galleries = soup.find('div', class_='galleries').find_all('a')
            for gallery in galleries:
                venue_list.append(self.domain + gallery['href'])
        except Exception as ex:
            print(ex)

        event_list = []
        try:
            events = soup.findAll(class_='exhib-title')
            for event in events:
                event_list.append(self.domain + event['href'])
        except Exception as ex:
            print(ex)

        person['category'] = category
        person['url'] = url
        person['name'] = name
        person['bio'] = bio
        person['venue_list'] = venue_list
        person['event_list'] = event_list
        person['scrape_time'] = datetime.datetime.now()

        # for url in venue_list:
        #     yield scrapy.Request(url=url, callback=self.parse)
        #
        # for url in event_list:
        #     yield scrapy.Request(url=url, callback=self.parse)

        yield person
