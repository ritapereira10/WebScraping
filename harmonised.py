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
db = client['petsOld']
products = db['dryDogFood']
db2 = client['petsScraping']
products2 = db2['foodProducts']
#######################################################################

dfAll = pd.DataFrame.from_records(products.find())
dfPets = dfAll[dfAll['Store']=='Pets_at_Home']
dfPets = dfPets.sort_values('Date')
dfPets = dfPets[dfPets['Date']==dfPets.iloc[-1]['Date']]
dfPets['Brand'] = dfPets['Brand'].str.strip()
dfPets['title'] = (dfPets['Brand'].map(str) + ' ' + dfPets['Description'].map(str)).str.lower()
dfPets['title'] = dfPets['title'].str.replace("'","")
dfPets['title'] = dfPets['title'].str.replace("purina an","pro plan")
dfPets['title'] = dfPets['title'].str.replace("with","")
dfPets['title'] = dfPets['title'].str.replace("skinners field and trial complete dog food 15kg","skinners ruff and ready complete dog food 15kg")
dfPets['title'] = dfPets['title'].str.replace("fishmongers finest og","fishmongers adult dog")
titles = dfPets['title'].tolist()
print(len(dfPets))

def applyInitialAdjustments(title, size):
    if 'rehydrated' in title.lower():
        rehydratedSize = re.search('\(([^)]+)', title.lower()).group(1)
        rehydratedSize = re.search(r'(.*?)kg', rehydratedSize)
        return float(rehydratedSize.group(1))
    else:
        return size

def applyPromo(promo, size, price, pricePerKilo):
    for p in promo:
        p = p.lower().strip()

        sizeMatch = re.search(r'on (.*?)kg', p)
        if sizeMatch is not None:
            if float(sizeMatch.group(1).strip()) != size:
                continue
            else:
                p = p.replace(sizeMatch.group(0), '').strip()

        sizeMatchGrams = re.search(r'on (.*?)g', p)
        if sizeMatchGrams is not None:
            if (float(sizeMatchGrams.group(1).strip()) / 1000) != size:
                continue
            else:
                p = p.replace(sizeMatchGrams.group(0), '').strip()

        if p == 'buy one get one half price':
            return (price * 1.5 / (size * 2))

        kgFree = re.search(r'plus (.*?)kg free', p)
        if kgFree is not None:
            return (price / (size + float(kgFree.group(1).strip())))

        p = p.replace('price cut - ', '')
        p = p.replace('online - ', '')

        nowSaveFilter = re.search(r'now £(.*?),', p)
        if nowSaveFilter is not None:
            return (float(nowSaveFilter.group(1).strip()) / size)

        nowFilter = re.search(r'now £(.*)', p)
        if nowFilter is not None:
           return (float(nowFilter.group(1).strip()) / size)

        nowFilterPence = re.search(r'now (.*?)p', p)
        if nowFilterPence is not None:
            return ((float(nowFilterPence.group(1).strip()) / 100) / size)

        xForYPounds = re.search(r' for £(.*)', p)
        if xForYPounds is not None:
            p = p.replace('buy', '').strip()
            numberOfItems = float(p.split('for')[0].strip())
            specialPrice = float(xForYPounds.group(1).strip())
            return (specialPrice / (numberOfItems * size))

        if 'for the price of' in p:
            pItems = p.split('for the price of')
            return (float(pItems[1].strip()) * price) / (float(pItems[0].strip()) * size)

        if 'each when you buy' in p:
            p = p.replace('online', '').split('each when you buy')
            if '£' in p[0]:
                return (float(p[0].strip().replace('£', '')) / size)
            elif 'p' in p[0]:
                return ( (float(p[0].strip().replace('p', ''))/100) / size)

        if len(p.split(' ')) == 3 and p.split(' ')[1].strip() == 'for':
            return ((float(p.split(' ')[2].strip()) * price) / (float(p.split(' ')[0].strip()) * size))

        if 'save' in p:
            p = re.search(r'save (.*)', p)
            discount = p.group(1).strip()
            if '£' in discount:
                return ((price - float(discount.replace('£', ''))) / size)
            elif '%' in discount:
                discount = (100 - float(discount.replace('%', ''))) / 100
                return (discount * pricePerKilo)

        #print(p)

    return pricePerKilo

dfNew = pd.DataFrame.from_records(products2.find())
dfNew['size'] = dfNew.apply(lambda row: applyInitialAdjustments(row['title'], row['size']), axis=1)
dfNew['pricePerKilo'] = dfNew['price'] / dfNew['size']
dfNew['pricePerKiloAfterPromo'] = dfNew.apply(lambda row: applyPromo(row['promo'], row['size'], row['price'], row['pricePerKilo']), axis=1)
dog = dfNew[dfNew['categories'].apply(lambda x: 'dog' in x)]
dfNew = dog[dog['categories'].apply(lambda x: 'dry-dog-food' in x)].copy()
dfNew['title'] = dfNew['title'].str.lower()
dfNew['title'] = dfNew['title'].str.replace("'","")
dfNew['title'] = dfNew['title'].str.replace("with","")
dfNew['title'] = dfNew['title'].str.replace("hills science plan","hills prescription")

dfFinal2 = dfNew[dfNew['dateTimeOfScrape']==dfNew.iloc[0]['dateTimeOfScrape']]
titles2 = dfFinal2['title'].tolist()

result = dfFinal2[dfFinal2['title'].isin(titles)]
expo = dfPets.merge(result, how='left', on='title')
expo['difference'] = expo['pricePerKilo'] - expo['Price per KG']
#expo.to_csv('C:\\temp\\expo.csv')
print(expo['difference'].mean())
print(dfPets['Price per KG'].mean())
print(result['pricePerKilo'].mean())
print(result['pricePerKiloAfterPromo'].mean())

