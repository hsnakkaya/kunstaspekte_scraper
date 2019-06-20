import scrapy


class PersonItem(scrapy.Item):
    category = scrapy.Field()
    url = scrapy.Field()
    name = scrapy.Field()
    bio = scrapy.Field()
    venue_list = scrapy.Field()
    event_list = scrapy.Field()
    scrape_time = scrapy.Field()


class EventItem(scrapy.Item):
    category = scrapy.Field()
    url = scrapy.Field()
    name = scrapy.Field()
    venue = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    press_release = scrapy.Field()
    participant_list = scrapy.Field()
    scrape_time = scrapy.Field()


class VenueItem(scrapy.Item):
    category = scrapy.Field()
    url = scrapy.Field()
    name = scrapy.Field()
    connected_venue_list = scrapy.Field()
    event_list = scrapy.Field()
    artist_list = scrapy.Field()
    city = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    street_address = scrapy.Field()
    website = scrapy.Field()
    email = scrapy.Field()
    scrape_time = scrapy.Field()
