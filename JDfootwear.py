import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
import datetime
import pandas as pd
from more_itertools import unique_everseen
import numpy as np
import re
from itertools import repeat

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['jdScraping']
products = db['products']
########################################################################

######################## KEY VARIABLES ########################
writeToDB = True
dateTimeOfScrape = datetime.datetime.utcnow()
docs = []
###############################################################

class SpiderJDSports(CrawlSpider):
    name = 'jdSports',
    allowed_domains = ['jdsports.co.uk']
    start_urls = ['https://www.jdsports.co.uk/men/mens-footwear/', 'https://www.jdsports.co.uk/men/mens-clothing/', 'https://www.jdsports.co.uk/women/womens-footwear/', 'https://www.jdsports.co.uk/women/womens-clothing/', 'https://www.jdsports.co.uk/kids/?facet-category=footwear', 'https://www.jdsports.co.uk/kids/?facet:gender=girls&facet:category=clothing_', 'https://www.jdsports.co.uk/kids/?facet:gender=boys&facet:category=clothing_']

    custom_settings = {
        'LOG_ENABLED': 'False',
        'ROBOTSTXT_OBEY': 'False',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        #'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//a[contains(@rel, "next")]'),),follow=True),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//ul[contains(@id, "productListMain")]'),),callback='parse_item'),
    )

    def parse_item(self, response):
        categories = []
        brand = ''
        title = response.xpath('//div[@id="productItemTitle"]/h1/text()').extract_first()
        storePrice = response.xpath('//div[@class="itemPrices"]/meta[@itemprop="price"]/@content').extract_first()
        #storePrice = response.xpath('//div[@class="itemPrices"]/span/text()').extract_first()
        productCode = response.xpath('//span[@class="product-code"]/text()').extract_first().lower().replace('product code: ','')
        colour = response.xpath('//div[@class="tabInf"]/text()').extract()[2].strip()

        rows = response.xpath('//div[@id="itemRelatedCats"]/ul/li')
        for r in rows:
            categories.extend(r.xpath('a/text()').extract())
        categories = list(unique_everseen(categories))
        if len(categories) > 1:
            brand = categories[1]
        print(title)

        doc = {
            'dateTimeOfScrape': dateTimeOfScrape,
            'categories': categories,
            'brand': brand,
            'title': title,
            'storePrice': storePrice,
            'colour': colour,
            'productCode': productCode
        }
        docs.append(doc)


process = CrawlerProcess()
process.crawl(SpiderJDSports)
process.start()

final = pd.DataFrame.from_records(docs)
final['unique'] = final['productCode'].map(str) + final['colour'].map(str)
final.drop_duplicates('unique', inplace=True)
final.drop('unique', axis=1, inplace=True)
print(final)

if writeToDB == True:
    products.insert_many(final.to_dict('records'))