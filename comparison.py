from pymongo import MongoClient
import datetime
import pandas as pd
import numpy as np
import re
from itertools import repeat
from bs4 import BeautifulSoup
from selenium import webdriver
import time

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['petsScraping']
products = db['foodProducts']
dbAmazon = client['amazonScraping']
comparisons = dbAmazon['popularDryDogFood']
########################################################################

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

def applyInitialAdjustments(title, size):
    if 'rehydrated' in title.lower():
        rehydratedSize = re.search('\(([^)]+)', title.lower()).group(1)
        rehydratedSize = re.search(r'(.*?)kg', rehydratedSize)
        return float(rehydratedSize.group(1))
    else:
        return size

def processTitle(title):
    title = title.lower()
    title = title.replace(',', ' ')
    title = title.replace('&', '')
    title = title.replace('and', '')
    title = title.replace('kg', '')
    title = title.replace('kilograms', '')
    title = title.replace('&', 'and')
    title = title.replace('for', '')
    title = title.replace('by', '')
    title = title.replace('dogs', '')
    title = title.replace('dog', '')
    title = title.replace('food', '')
    title = title.replace('with', '')
    title = title.replace('diets', 'diet')
    title = title.replace('vegetable', 'veg')
    title = title.replace('(', '')
    title = title.replace(')', '')
    title = title.replace('/', ' ')
    title = title.replace('-', ' ')
    title = title.replace("'", "")
    title = title.replace('´', '')
    title = title.split(' ')
    title = [s.strip() for s in title]
    title = [s for s in title if s]
    return title

def addSize(size, text):
    if 'kg' not in text.lower():
        return text + ' ' + str('%g'%(size))
    return text

dfAll = pd.DataFrame.from_records(products.find())
dog = dfAll[dfAll['categories'].apply(lambda x: 'dog' in x)]
dryDogFood = dog[dog['categories'].apply(lambda x: 'dry-dog-food' in x)].copy()
dryDogFood = dryDogFood[dryDogFood['dateTimeOfScrape'] == dryDogFood.iloc[-1]['dateTimeOfScrape']]
dryDogFood['title'] = dryDogFood.apply(lambda row: addSize(row['size'], row['title']), axis=1)
dryDogFood['title'] = dryDogFood['title'].apply(processTitle)

def compareTitles(title):
    for t, p, s in zip(dryDogFood['title'], dryDogFood['productCode'], dryDogFood['size']):
        if set(t).issubset(set(title)) or set(title).issubset(set(t)):
            return p + ' ' + str('%g'%(s))
    return ''

def round_of_rating(number):
    return round(number * 2) / 2

amazonPopular = pd.DataFrame.from_records(comparisons.find())
amazonPopular['size'] = (amazonPopular['price'] / amazonPopular['pricePerKilo']).apply(round_of_rating)
amazonPopular['title'] = amazonPopular.apply(lambda row: addSize(row['size'], row['title']), axis=1)
amazonPopular['title'] = amazonPopular['title'].apply(processTitle)
amazonPopular['productCode'] = amazonPopular['title'].apply(compareTitles)
#print(amazonPopular)
dryDogFoodJoin = dryDogFood[['productCode', 'price', 'size']].copy()
dryDogFoodJoin['productCode'] = dryDogFoodJoin.apply(lambda row: addSize(row['size'], row['productCode']), axis=1)
dryDogFoodJoin.rename(columns={'price':'petsPrice'}, inplace=True)
result = pd.merge(amazonPopular, dryDogFoodJoin, on='productCode')
result = result[['title', 'productCode', 'price', 'petsPrice', 'size_y']]
result['difference'] = (result['petsPrice'] - result['price']) / result['size_y']
print(result['difference'].mean())

#print(dryDogFood.iloc[0]['title'])