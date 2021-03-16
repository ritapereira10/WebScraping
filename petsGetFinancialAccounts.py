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


vets = pd.read_csv('C:\\temp\\vetPracticeCodesFinal.csv', encoding = "ISO-8859-1")
vets['companyCode'] = vets['companyCode'].astype(str)
vets['incorporation date'] = pd.to_datetime(vets['incorporation date'])
print(vets)
vetList = vets['companyCode'].tolist()
vetList = ['https://beta.companieshouse.gov.uk/company/' + s.strip() + '/filing-history?page=1' for s in vetList]
print(vetList)
vets['companyCode'] = vets['companyCode'].str.strip()
vets.set_index('companyCode', inplace=True)
vets['incorporation date'] = pd.to_datetime(vets['incorporation date'])
vets['year'] = vets['incorporation date'].dt.year


def download_file(download_url, name, year, company):
    if not os.path.exists('C:\\temp\\accounts\\' + str(year) + '\\'):
        os.makedirs('C:\\temp\\accounts\\' + str(year))
    if not os.path.exists('C:\\temp\\accounts\\' + str(year) + '\\' + company):
        os.makedirs('C:\\temp\\accounts\\' + str(year) + '\\' + company)
    response = requests.get(download_url)
    file = open('C:\\temp\\accounts\\' + str(year) + '\\' + company + '\\' + name + '.pdf', 'wb')
    file.write(response.content)
    file.close()
    print("Completed")


class SpiderPetsFinancials(CrawlSpider):
    name = 'PetsFinancials',
    start_urls = vetList

    custom_settings = {
        'LOG_ENABLED': 'False',
    }

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//a[contains(@id, "nextButton")]'),),callback='parse_start_url', follow=True),

    )

    def parse_start_url(self, response):
        print(response.url)
        companyCode = response.xpath('//p[contains(@id, "company-number")]/strong/text()').extract_first()
        incYear = vets.loc[companyCode]['year']
        #print(incYear)
        name = vets.loc[companyCode]['company']
        #print(name)
        rows = response.xpath('//tr')
        for row in rows:

            print(row)
            temp = row.xpath('td/strong/text()').extract()
            print(len(temp))
            print(temp[0])
            temp2 = row.xpath('td/text()').extract()
            temp3 = row.xpath('td/div/a/@href').extract_first()
            if temp is not None and len(temp) > 0:
                if 'accounts' in temp[0].lower():
                    fileName = temp[0] + ' ' + temp2[3].strip()
                    download_file('https://beta.companieshouse.gov.uk/' + temp3, fileName, incYear, name)
        print('')


process = CrawlerProcess()
process.crawl(SpiderPetsFinancials)
process.start()