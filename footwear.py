from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
import datetime
import pandas as pd
from more_itertools import unique_everseen

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['sportsScraping']
products = db['products']
########################################################################

######################## KEY VARIABLES ########################
writeToDB = False
dateTimeOfScrape = datetime.datetime.utcnow()
docs = []
###############################################################

class SpiderSportsDirect(CrawlSpider):
    name = 'SportsDirect',
    allowed_domains = ['sportsdirect.com']
    start_urls = ['http://www.sportsdirect.com/mens/mens-trainers', 'http://www.sportsdirect.com/football/football-boots/mens-football-boots', 'http://www.sportsdirect.com/running/running-shoes/mens-running-shoes/all-mens-running-shoes', 'http://www.sportsdirect.com/football/football-boots/astro-trainers', 'http://www.sportsdirect.com/mens/mens-walking-boots-and-shoes', 'http://www.sportsdirect.com/mens/mens-boots', 'http://www.sportsdirect.com/ladies/ladies-fitness/ladies-training-footwear', 'http://www.sportsdirect.com/ladies/ladies-trainers', 'http://www.sportsdirect.com/ladies/ladies-boots', 'http://www.sportsdirect.com/ladies/ladies-walking-boots', 'http://www.sportsdirect.com/kids/kids-trainers', 'http://www.sportsdirect.com/football/football-boots/astro-trainers', 'http://www.sportsdirect.com/kids/kids-trainers', 'http://www.sportsdirect.com/mens/mens-jackets-and-coats', 'http://www.sportsdirect.com/mens/mens-tracksuits', 'https://www.sportsdirect.com/mens/mens-hoodies', 'http://www.sportsdirect.com/mens/mens-t-shirts', 'http://www.sportsdirect.com/mens/mens-tracksuit-bottoms', 'https://www.sportsdirect.com/running/running-clothes/mens-running-clothes/all-mens-running-clothes', 'http://www.sportsdirect.com/ladies/ladies-tights-and-leggings', 'http://www.sportsdirect.com/ladies/ladies-jackets-and-coats', 'http://www.sportsdirect.com/ladies/ladies-fitness/ladies-workout-pants-and-shorts', 'http://www.sportsdirect.com/ladies/ladies-fitness/ladies-sports-bras', 'http://www.sportsdirect.com/ladies/ladies-hoodies', 'http://www.sportsdirect.com/ladies/ladies-tracksuits', 'http://www.sportsdirect.com/kids/kids-tracksuits', 'http://www.sportsdirect.com/kids/kids-jackets-and-coats', 'http://www.sportsdirect.com/kids/kids-hoodies', 'http://www.sportsdirect.com/kids/kids-base-layer', 'http://www.sportsdirect.com/kids/kids-character-clothing', 'http://www.sportsdirect.com/kids/kids-t-shirts']

    custom_settings = {
        'LOG_ENABLED': 'False',
        #'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//a[contains(@class, "swipeNextClick NextLink")]'),),follow=True),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//ul[contains(@id, "navlist")]'),),callback='parse_item'),
    )

    def parse_item(self, response):
        categories = []
        brand = ''
        title = response.xpath('//span[@id="lblProductName"]/text()').extract_first()
        productCode = response.xpath('//div[@class="infoTabPage"]/span/p/text()').extract_first().replace('Product code: ','')
        colour = response.xpath('//span[@id="dnn_ctr103511_ViewTemplate_ctl00_ctl10_colourName"]/text()').extract_first()
        storePrice = response.xpath('//div[@class="pdpPrice"]/span/text()').extract_first()
        normalPrice = response.xpath('//div[@class="originalprice"]/span/text()').extract_first()
        print(title)
        rows = response.xpath('//li[@class="MoreFromLinksRow"]')
        firstRow = True
        for r in rows:
            rowItems = r.xpath('a/text()').extract()
            if firstRow == True:
                brand = rowItems[0]
                firstRow = False
            else:
                for item in rowItems:
                    categories.append(item)
        categories = list(unique_everseen(categories))

        if normalPrice is None:
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
        else:
            doc = {
                'dateTimeOfScrape': dateTimeOfScrape,
                'categories': categories,
                'brand': brand,
                'title': title,
                'storePrice': storePrice,
                'normalPrice': normalPrice,
                'colour': colour,
                'productCode': productCode
            }
            docs.append(doc)


process = CrawlerProcess()
process.crawl(SpiderSportsDirect)
process.start()

final = pd.DataFrame.from_records(docs)
final['unique'] = final['productCode'].map(str) + final['colour'].map(str)
final.drop_duplicates('unique', inplace=True)
final.drop('unique', axis=1, inplace=True)

if writeToDB == True:
    products.insert_many(final.to_dict('records'))