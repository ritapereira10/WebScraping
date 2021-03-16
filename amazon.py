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
comparisons = dbAmazon['petsDryDogFoodComparison']
########################################################################

######################## KEY VARIABLES ########################
sleepConstant = 2
writeToDB = True
dateTimeOfComparison = datetime.datetime.utcnow()
docs = []
###############################################################

####### Get list of dry dog food products #####################
def applyInitialAdjustments(title, size):
    title = title.replace('(Web Exclusive)', '')
    if 'kg' in title.lower():
        return title
    else:
        return title + ' ' + str('%g'%(size)) + 'kg'

dfAll = pd.DataFrame.from_records(products.find())
dog = dfAll[dfAll['categories'].apply(lambda x: 'dog' in x)]
dryDogFood = dog[dog['categories'].apply(lambda x: 'dry-dog-food' in x)].copy()
dryDogFood['title'] = dryDogFood.apply(lambda row: applyInitialAdjustments(row['title'], row['size']), axis=1)
dryDogFood.drop_duplicates('title', inplace=True)
dryDogFood['products'] = dryDogFood[['title', 'productCode', 'size']].apply(tuple, axis=1)
products = dryDogFood['products'].tolist()
print(products)
###############################################################

def processTitle(title):
    title = title.lower()
    title = title.replace(',', ' ')
    title = title.replace('&', 'and')
    title = title.replace('kg', '')
    title = title.replace('kilograms', '')
    title = title.replace('&', 'and')
    title = title.replace('for', '')
    title = title.replace('by', '')
    title = title.replace('dogs', '')
    title = title.replace('dog', '')
    title = title.replace('food', '')
    title = title.replace('with', '')
    title = title.replace('complete', '')
    title = title.replace('diets', 'diet')
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

driver = webdriver.Chrome('C:\\python\\chromedriver')
for product in products:
    driver.get('http://www.amazon.co.uk/')
    search = driver.find_element_by_xpath("//input[@id='twotabsearchtextbox']")
    search.send_keys(product[0])
    search.find_element_by_xpath("//input[@class='nav-input']").click()
    noResults = None
    try:
        noResults = driver.find_element_by_xpath("//h1[@id='noResultsTitle']")
    except:
        pass
    if noResults is not None:
        time.sleep(sleepConstant)
        continue
    else:
        try:
            titleResult = driver.find_elements_by_xpath("//h2[@class='a-size-medium s-inline  s-access-title  a-text-normal']")[0].text
            print(processTitle(titleResult))
            print(processTitle(product[0]))
            if set(processTitle(titleResult)).issubset(set(processTitle(product[0]))) or set(processTitle(product[0])).issubset(set(processTitle(titleResult))):
                price = driver.find_elements_by_xpath("//span[@class='a-size-base a-color-price s-price a-text-bold']")[0].text.replace('£','').strip()
                print(price)
                if writeToDB == True:
                    doc = {
                        'dateTimeOfComparison': dateTimeOfComparison,
                        'amazonTitle': titleResult,
                        'amazonPrice': float(price),
                        'productCode': product[1],
                        'amazonSize': product[2]
                    }
                    comparisons.insert_one(doc)

            print('')

            time.sleep(sleepConstant)
        except:
            time.sleep(sleepConstant)
            pass

driver.quit()
