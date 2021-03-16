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
db = client['petsScraping']
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

class SpiderPetsAtHome(CrawlSpider):
    name = 'PetsAtHome',
    allowed_domains = ['petsathome.com']
    start_urls = ['http://www.petsathome.com/shop/en/pets/dog/dog-food-and-treats', 'http://www.petsathome.com/shop/en/pets/puppy/puppy-food-and-treats', 'http://www.petsathome.com/shop/en/pets/cat/cat-food-and-treats', 'http://www.petsathome.com/shop/en/pets/kitten/kitten-food-and-treats']

    custom_settings = {
        'LOG_ENABLED': 'False',
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }

    rules = (
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
        promo = response.xpath('//div[@class="product-sku-detail"]/p[@class="promo-text"]')#/a/strong/text()').extract()
        if promo is None:
            promo = []
        else:
            tempPromo = []
            for p in promo:
                tempPromo.append(p.xpath('a/strong/text()').extract_first())
            promo = tempPromo
        longDescription = response.xpath('//div[@id="tab1_content"]/p/text()').extract()
        if longDescription is not None:
            longDescription = ' '.join(longDescription)
        else:
            longDescription = ''
        nutrition = response.xpath('//div[@id="tab3_content"]/p/text()').extract()
        if nutrition is not None:
            nutrition = ' '.join(nutrition)
        else:
            nutrition = ''
        topPrice = response.xpath('//div[@class="fr col-right"]/div/span/text()').extract_first().strip().replace('£', '')

        rows = response.xpath('//table[@class="food-table"]/tbody/tr')
        for row in rows:
            temp1 = row.xpath('td/span/text()').extract()
            temp1 = [x.replace('\n', '') for x in temp1]
            temp2 = row.xpath('td/text()').extract()
            temp2 = [x.replace('\n', '') for x in temp2]
            if len(temp2) > 0:
                size = temp2[0]
                if size != '':
                    if 'kg' in size:
                        size = float(size.replace('kg',''))
                    else:
                        size = float(size.replace('g', '')) / 1000.0

                    if len(temp1) > 0:
                        price = float(temp1[0].replace('£',''))
                        pricePerKilo = float(temp2[5].replace('£',''))
                    else:
                        price = float(topPrice)
                        pricePerKilo = price / size

                    print(title)

                    doc = {
                        'dateTimeOfScrape': dateTimeOfScrape,
                        'store': 'Pets At Home',
                        'categories': categories,
                        'title': title,
                        'description': longDescription,
                        'nutrition': nutrition,
                        'promo': promo,
                        'productCode': productCode,
                        'size': size,
                        'price': price,
                        'pricePerKilo': pricePerKilo
                    }
                    docs.append(doc)


process = CrawlerProcess()
process.crawl(SpiderPetsAtHome)
process.start()

final = pd.DataFrame.from_records(docs)

final['categories'] = final.groupby('productCode')['categories'].transform(mergeCategories)

final['unique'] = final['productCode'].map(str) + final['size'].map(str)
final = final.drop_duplicates('unique')
final.drop(['unique'], axis=1, inplace=True)

if writeToDB == True:
    products.insert_many(final.to_dict('records'))
