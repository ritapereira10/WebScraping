import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
import datetime
import re

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['petsScraping']
products = db['allProducts']
########################################################################

######################## KEY VARIABLES ########################
writeToDB = True
dateTimeOfScrape = datetime.datetime.utcnow()
###############################################################

items = set()


class SpiderPetsAtHome(CrawlSpider):
    name = 'PetsAtHome',
    allowed_domains = ['petsathome.com']
    start_urls = ['http://www.petsathome.com']

    custom_settings = {
        'LOG_ENABLED': 'False',
    }

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//ul[contains(@class, "menu level-1")]'), tags='li', attrs='url'),),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//ul[contains(@class, "category-list dotted-wrap bottom")]'),),),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//a[contains(@title, "Next Page")]'),),),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//p[contains(@class, "description")]'),),callback='parse_item'),

    )

    def parse_item(self, response):
        referer = response.request.headers.get('Referer', None).decode("utf-8")
        referer = referer.replace('http://www.petsathome.com/shop/en/pets/','')
        referer = referer.split('?')[0]
        categories = referer.split('/')
        title = response.xpath('//h1[@class="h7 product-title"]/text()').extract_first()
        productCode = response.xpath('//p[@class="stock-details"]/span[@class="product-code"]/text()').extract_first()

        if productCode not in items:
            items.add(productCode)
            print(title)
            if (writeToDB == True):
                doc = {
                    'dateTimeOfScrape': dateTimeOfScrape,
                    'store': 'Pets At Home',
                    'categories': categories,
                    'title': title,
                    'productCode': productCode
                }
                products.insert_one(doc)


process = CrawlerProcess()
process.crawl(SpiderPetsAtHome)
process.start()