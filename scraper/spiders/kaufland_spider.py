import json
import requests
import scrapy
import urllib.parse

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

class KauflandSpider(scrapy.Spider):
    name = "kaufland"
    allowed_domains = ["kaufland.de"]
    base_url = "https://www.kaufland.de"
    start_urls = []
    frontend_be_url = "https://api.cloud.kaufland.de/pdp-frontend/v1/{id}/{endpoint}"
    custom_settings = {
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "proxy": {
                "server": "http://proxy.scrapeops.io:5353",
                "username": "scrapeops",
                "password": "a9d85a82-c14b-4b9c-952f-2dce4d6a16f9",
            },
        }
    }
    i = 0

    def __init__(self, product_name, no_pages=1, is_prod=False, *a, **kw):
        self.current_url = ""
        if not is_prod:
            self.start_urls.append("https://www.kaufland.de/laptops/")
        else:
            for page_no in range(1, no_pages + 1):
                self.start_urls.append(f"https://www.kaufland.de/item/search/?search_value="
                                       f"{urllib.parse.quote(product_name)}&page={page_no}")
        super().__init__(*a, **kw)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, self.parse, meta=dict(playwright=True,
                                                            playwright_context_kwargs={
                                                                "ignore_https_errors": True,
                                                            }))

    def parse(self, response, **kwargs):
        self.current_url = response.url
        product_paths = response.css("a.product__wrapper::attr(href)").getall()
        for path in product_paths:
            yield scrapy.Request(
                self.base_url + path,
                callback=self.parse_product,
                meta=dict(playwright=True,
                          playwright_context_kwargs={
                              "ignore_https_errors": True,
                          }))

    def parse_product(self, response):
        # TODO: convert this to regex
        product_id = response.url.split("/")[-2]
        print(product_id)
        reviews_json = requests.get(url=self.frontend_be_url.format(id=product_id, endpoint='reviews/')).json()
        variants_json = requests.get(url=self.frontend_be_url.format(id=product_id, endpoint='variants')).json()

        item = KauflandProduct()
        item['title'] = response.css("h1.rd-title::attr(title)").get().strip()
        item['price'] = float(response.css(".rd-price-information__price *::text").get()
                              .strip().removesuffix('€').replace('.', '').replace(',', '.'))
        item['seller_name'] = response.css("span[data-pw='seller-name'] *::text").get().strip()
        item['shipping'] = response.css(".rd-shipping-return *::text").get()
        item['availability'] = response.css("span[data-pw='delivery-time'] *::text").get().strip()
        item['short_description'] = response.css(".rd-description-teaser *::text").getall()
        item['variants'] = json.dumps(variants_json)
        item['reviews'] = json.dumps(reviews_json['reviews'])
        item['no_stars'] = reviews_json['averageRating']
        item['no_reviews'] = reviews_json['totalReviews']

        print("Product:")
        print("Title: " + item['title'])
        print("Price: " + str(item['price']))
        print("Seller Name: " + item['seller_name'])
        if item['shipping'] is not None:
            print("Shipping: " + item['shipping'])
        print("Availability: " + item['availability'])
        print("Short Description:" + " ".join(item['short_description']))
        print("Variants" + item['variants'])
        print("Reviews:")
        print(item['reviews'])
        print("No of stars: " + str(item['no_stars']))
        print("No of Reviews: " + str(item['no_reviews']))

        yield item