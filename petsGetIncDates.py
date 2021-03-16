import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
import datetime
import pandas as pd
import re
from itertools import repeat
import requests
import os

docs = []

vets = pd.read_csv('C:\\temp\\vetPracticeCodes.csv', encoding = "ISO-8859-1")
vetList = vets['companyCode'].tolist()
vetList = ['https://beta.companieshouse.gov.uk/company/' + s.strip() for s in vetList]
print(vetList)
vets['companyCode'] = vets['companyCode'].str.strip()
#vets.set_index('companyCode', inplace=True)

class SpiderPetsFinancials(CrawlSpider):
    name = 'PetsFinancials',
    start_urls = vetList

    custom_settings = {
        'LOG_ENABLED': 'False',
    }

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//a[contains(@id, "nextButton")]'),),callback='parse_start_url', follow=False),

    )

    def parse_start_url(self, response):
        print(response.url)
        incDate = pd.to_datetime(response.xpath('//dd[contains(@id, "company-creation-date")]/text()').extract_first())
        companyCode = response.xpath('//p[contains(@id, "company-number")]/strong/text()').extract_first()
        companyName = response.xpath('//p[contains(@id, "company-number")]/strong/text()').extract_first()
        doc = {
            'companyCode': companyCode,
            'incorporation date': incDate,
        }
        docs.append(doc)

        '''incYear = vets.loc[companyCode]['year']
        print(incYear)
        if incYear > 2016:
            name = vets.loc[companyCode]['company']
            print(name)
            rows = response.xpath('//tr')
            for row in rows:
                temp = row.xpath('td/strong/text()').extract()
                temp2 = row.xpath('td/text()').extract()
                temp3 = row.xpath('td/div/a/@href').extract_first()
                if temp is not None and len(temp) > 0:
                    if 'accounts' in temp[0].lower():
                        fileName = temp[0] + ' ' + temp2[3].strip()
                        download_file('https://beta.companieshouse.gov.uk/' + temp3, fileName, incYear, name)
            print('')'''


process = CrawlerProcess()
process.crawl(SpiderPetsFinancials)
process.start()

final = pd.DataFrame.from_records(docs)
print(final)
print(vets)
final = vets.merge(final,on='companyCode')[['company', 'companyCode', 'incorporation date']]

final.to_csv('C:\\temp\\vetPracticeCodesFinal.csv')