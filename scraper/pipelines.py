from datetime import datetime

from sqlalchemy import create_engine, Table, Column, MetaData, JSON, INTEGER, TIMESTAMP, NUMERIC, SMALLINT, ARRAY, \
    VARCHAR, insert

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

BATCH_SIZE = 30


class PostgresDemoPipeline:
    kaufland_batch = []
    home24_batch = []
    galaxus_batch = []

    def __init__(self):
        print("gurbet")
        hostname = 'localhost'
        username = 'shopvibes'
        password = 'shopvibes'
        database = 'shopvibes'

        self.engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(username, password, hostname, database))

        metadata = MetaData()
        self.product_data_table = Table('product_data', metadata,
                                        Column('id', INTEGER, primary_key=True, autoincrement=True),
                                        Column('product_id', VARCHAR),
                                        Column('image_url', VARCHAR),
                                        Column('product_url', VARCHAR),
                                        Column('url', VARCHAR),
                                        Column('source', VARCHAR),
                                        Column('keywords', VARCHAR),
                                        Column('position', INTEGER),
                                        Column('ean', VARCHAR),
                                        Column('sku', VARCHAR),
                                        Column('product_title', VARCHAR),
                                        Column('shop_name', VARCHAR),
                                        Column('shop_id', VARCHAR),
                                        Column('crawling_timestamp', TIMESTAMP, default=datetime.utcnow),
                                        Column('category_path', VARCHAR),
                                        Column('uvp', NUMERIC),
                                        Column('current_price', NUMERIC),
                                        Column('discount', NUMERIC),
                                        Column('seller', VARCHAR),
                                        Column('shipping_cost', NUMERIC),
                                        Column('availability', VARCHAR),
                                        Column('product_details', JSON),
                                        Column('description', JSON),
                                        Column('avg_stars', NUMERIC),
                                        Column('recommendation_rate', SMALLINT),
                                        Column('reviews', JSON),
                                        Column('competitors', ARRAY(VARCHAR)),
                                        Column('variants', JSON),
                                        Column('meta_data', VARCHAR),
                                        )

    def process_item(self, item, spider):
        spider_class_name = spider.__class__.__name__
        if spider_class_name == 'KauflandSpider':
            self.process_item_Kaufland(item)
        elif spider_class_name == 'Home24Spider':
            self.process_item_Home24(item)
        elif spider_class_name == 'GalaxusSpider':
            self.process_item_Galaxus(item)
        else:
            print(f"Pipeline for {spider_class_name} not implemented yet")

    def close_spider(self, spider):
        spider_class_name = spider.__class__.__name__
        if spider_class_name == 'KauflandSpider':
            self.insertDB_Kaufland()
        elif spider_class_name == 'Home24Spider':
            self.insertDB_Home24()
        elif spider_class_name == 'GalaxusSpider':
            self.insertDB_Galaxus()
        else:
            print(f"Pipeline for {spider_class_name} not implemented yet")

    def process_item_Kaufland(self, item):
        print("Add to Batch Kaufland")
        self.kaufland_batch.append(item)
        if len(self.kaufland_batch) >= BATCH_SIZE:
            self.insertDB_Kaufland()

    def process_item_Home24(self, item):
        print("Add to Batch Home24")
        self.home24_batch.append(item)
        if len(self.home24_batch) >= BATCH_SIZE:
            self.insertDB_Home24()

    def process_item_Galaxus(self, item):
        print("Add to Batch Galaxus")
        self.galaxus_batch.append(item)
        if len(self.galaxus_batch) >= BATCH_SIZE:
            self.insertDB_Home24()

    def insertDB_Kaufland(self):
        if len(self.kaufland_batch) == 0:
            return
        print("Batch Insert")
        batch_insert_stmt = insert(self.product_data_table).values(
            [{'product_title': item['title'], 'current_price': item['price'], 'seller': item['seller_name'],
              'availability': item['availability'], 'description': item['short_description'],
              'variants': item['variants'], 'reviews': item['reviews'], 'avg_stars': item['no_stars']}
             for item in self.kaufland_batch]
        )
        with self.engine.begin() as conn:
            conn.execute(batch_insert_stmt)
        self.kaufland_batch.clear()

    def insertDB_Home24(self):
        if len(self.home24_batch) == 0:
            return
        print("Batch Insert")
        batch_insert_stmt = insert(self.product_data_table).values(
            [{'product_title': item['title'], 'current_price': item['price'], 'seller': item['seller_name'],
              'description': item['short_description'], 'reviews': item['reviews'], 'avg_stars': item['no_stars']}
             for item in self.home24_batch]
        )
        with self.engine.begin() as conn:
            conn.execute(batch_insert_stmt)
        self.home24_batch.clear()

    def insertDB_Galaxus(self):
        if len(self.galaxus_batch) == 0:
            return
        print("Batch Insert")
        batch_insert_stmt = insert(self.product_data_table).values(
            [{'product_title': item['title'], 'current_price': item['price'], 'seller': item['seller_name'],
              'description': item['short_description'], 'reviews': item['reviews'], 'avg_stars': item['no_stars']}
             for item in self.galaxus_batch]
        )
        with self.engine.begin() as conn:
            conn.execute(batch_insert_stmt)
        self.galaxus_batch.clear()