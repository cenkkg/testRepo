# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class KauflandProduct(scrapy.Item):
    title = scrapy.Field()
    price = scrapy.Field()
    seller_name = scrapy.Field()
    shipping = scrapy.Field()
    availability = scrapy.Field()
    short_description = scrapy.Field()
    variants = scrapy.Field()
    reviews = scrapy.Field()
    no_stars = scrapy.Field()
    no_reviews = scrapy.Field()
