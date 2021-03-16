from pymongo import MongoClient
import datetime
import pandas as pd
import re
from itertools import repeat
from selenium import webdriver
import time

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['amazonScraping']
products = db['popularDryDogFood']
########################################################################

######################## KEY VARIABLES ########################
writeToDB = True
dateTimeOfScrape = datetime.datetime.utcnow()
docs = []
sleepConstant = 1
###############################################################


class Item:
    def __init__(self, rank, url):
        self.rank = rank
        self.url = url
        self.title = None
        self.price = None
        self.priceperkilo = None

    def setTitle(self,title):
        self.title = title

    def setPrice(self,price):
        self.price = price

    def setPricePerKilo(self,priceperkilo):
        self.priceperkilo = priceperkilo


driver = webdriver.Chrome('C:\\python\\chromedriver')
driver.get('https://www.amazon.co.uk/Best-Sellers-Pet-Supplies-Dry-Dog-Food/zgbs/pet-supplies/13154141031')
pageLinks = driver.find_elements_by_xpath("//li[@class='zg_page ']/a")
links = []
for l in pageLinks:
    links.append(l.get_attribute("href"))

pageItems = driver.find_elements_by_xpath("//div[@class='zg_itemImmersion']")
items = []
for p in pageItems:
    items.append(Item(p.text.split('.')[0], p.find_element_by_class_name('a-link-normal').get_attribute("href")))
print(items)
for l in links:
    driver.get(l)
    pageItems = driver.find_elements_by_xpath("//div[@class='zg_itemImmersion']")
    for p in pageItems:
        items.append(Item(p.text.split('.')[0], p.find_element_by_class_name('a-link-normal').get_attribute("href")))
    time.sleep(sleepConstant)

for i in range(len(items)):
    driver.get(items[i].url)
    items[i].setTitle(driver.find_element_by_xpath("//span[@id='productTitle']").text)
    price = None
    try:
        price = driver.find_element_by_xpath("//span[@id='priceblock_ourprice']").text
    except:
        try:
            price = driver.find_element_by_xpath("//span[@id='priceblock_dealprice']").text
        except:
            pass
    priceperkilo = None
    try:
        priceperkilo = driver.find_element_by_xpath("//td[@class='a-span12']/span[@class='a-size-small a-color-price']").text
    except:
        try:
            priceperkilo = driver.find_element_by_xpath("//td[@class='a-span12']").text
        except:
            pass

    try:
        if price is not None:
            items[i].setPrice(float(price.replace('£','').strip()))
    except:
        pass

    try:
        if priceperkilo is not None:
            items[i].setPricePerKilo(float(priceperkilo.replace('£','').replace('/','').replace('kg','').split('(', 1)[1].split(')')[0]))
    except:
        pass

    print(items[i].title)
    print(items[i].price)
    print(items[i].priceperkilo)
    print('')

driver.quit()

amazonPopular = pd.DataFrame.from_records(products.find())

docs = []
for i in items:
    if i.priceperkilo is not None:
        try:
            titleMatch = amazonPopular[amazonPopular['title']==i.title]
            if len(titleMatch) > 0:
                doc = {
                    'dateTimeOfScrape': dateTimeOfScrape,
                    'store': 'Amazon',
                    'title': i.title,
                    'price': i.price,
                    'pricePerKilo': i.priceperkilo,
                    'productCode': titleMatch.iloc[0]['productCode']
                }
                docs.append(doc)
            else:
                doc = {
                    'dateTimeOfScrape': dateTimeOfScrape,
                    'store': 'Amazon',
                    'title': i.title,
                    'price': i.price,
                    'pricePerKilo': i.priceperkilo
                }
                docs.append(doc)
        except:
            pass

if writeToDB == True:
    final = pd.DataFrame.from_records(docs)
    products.insert_many(final.to_dict('records'))