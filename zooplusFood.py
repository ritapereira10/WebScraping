import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
import datetime
import pandas as pd
import re
from itertools import repeat

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['zooplusScraping']
products = db['foodProducts']
########################################################################

######################## KEY VARIABLES ########################
writeToDB = True
dateTimeOfScrape = datetime.datetime.utcnow()
docs = []
###############################################################

def mergeCategories(x):
    categoriesMerged = set()
    for l in x:
        categoriesMerged.update(l)
    finalCategories = list(categoriesMerged)
    result = [finalCategories for i in repeat(None, len(x))]
    return result


class SpiderZooplus(CrawlSpider):
    name = 'Zooplus',
    allowed_domains = ['zooplus.co.uk']
    start_urls = ['http://www.zooplus.co.uk/shop/dogs/dry_dog_food', 'http://www.zooplus.co.uk/shop/dogs/wet_dog_food', 'http://www.zooplus.co.uk/shop/dogs/dog_treats_chews', 'http://www.zooplus.co.uk/shop/cats/dry_cat_food', 'http://www.zooplus.co.uk/shop/cats/canned_cat_food_pouches', 'http://www.zooplus.co.uk/shop/cats/cat_treats_catnip']

    custom_settings = {
        'LOG_ENABLED': 'False',
        #'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//div[contains(@class, "category__content")]'),),),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//h3[contains(@class, "producttitle")]'),),callback='parse_item'),

    )

    def parse_item(self, response):
        referer = response.request.headers.get('Referer', None).decode("utf-8")
        referer = referer.replace('http://www.zooplus.co.uk/shop/','')
        referer = referer.split('?')[0]
        categories = referer.split('/')
        title = response.xpath('//h1[@class="producttitle h2"]/text()').extract_first()
        longDescription = response.xpath('//div[@id="description"]/div/div/article/text()').extract()
        if longDescription is not None:
            longDescription = ' '.join(longDescription)
        else:
            longDescription = ''
        nutrition = response.xpath('//div[@id="ingredients"]/div/div/article/text()').extract()
        if nutrition is not None:
            nutrition = ' '.join(nutrition)
        else:
            nutrition = ''

        rows = response.xpath('//div[@class="product__variants pd__variants js-productList "]/div[@class="clearfix product__offer "]')
        for row in rows:
            productCode = row.xpath('div[@class="product__varianttitle"]/div[@itemprop="sku"]/text()').extract_first()
            altTitle = row.xpath('div[@class="product__varianttitle"]/text()').extract_first()
            finalPrice = row.xpath('div[@class="product__prices_col prices "]/div/span[@class="price__amount"]/text()').extract_first()
            prices = row.xpath('div[@class="product__prices_col prices "]/span[@class="product__smallprice__text"]/text()').extract()
            pricePerKilo = ''
            beforePromo = ''
            if prices is not None and len(prices) > 0:
                pricePerKilo = prices[-1].strip()
                if len(prices) > 1:
                    beforePromo = prices[0].strip()

            print(title)
            doc = {
                'dateTimeOfScrape': dateTimeOfScrape,
                'store': 'Zooplus',
                'categories': categories,
                'title': title,
                'description': longDescription,
                'nutrition': nutrition,
                'beforePromo': beforePromo,
                'productCode': productCode.strip(),
                'altTitle': altTitle,
                'finalPrice': finalPrice,
                'pricePerKilo': pricePerKilo
            }
            docs.append(doc)

process = CrawlerProcess()
process.crawl(SpiderZooplus)
process.start()

final = pd.DataFrame.from_records(docs)

final['categories'] = final.groupby('productCode')['categories'].transform(mergeCategories)
final = final.drop_duplicates('productCode')
print(final)
if writeToDB == True:
    products.insert_many(final.to_dict('records'))
