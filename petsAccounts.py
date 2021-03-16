from pymongo import MongoClient
import datetime
import pandas as pd
import re
from itertools import repeat
from selenium import webdriver
import time

docs = []

driver = webdriver.Chrome('C:\\python\\chromedriver')

vets = pd.read_csv('C:\\temp\\vetShops.csv', encoding = "ISO-8859-1")
vetList = vets['Company'].tolist()
for v in vetList:
    try:
        driver.get('https://beta.companieshouse.gov.uk/')

        search = driver.find_element_by_xpath("//input[@id='site-search-text']")
        search.send_keys(v)
        search.find_element_by_xpath("//button[@class='search-submit']").click()

        searchResults = driver.find_elements_by_xpath("//ul[@id='results']/li/h3/a")
        result = searchResults[0].get_attribute("href")
        driver.get(result)
        incDate = driver.find_element_by_xpath("//p[@id='company-number']/strong").text + '   '
        doc = {
            'company': v,
            'companyCode': incDate,
        }
        docs.append(doc)
    except:
        pass

driver.quit()

final = pd.DataFrame.from_records(docs)
final.to_csv('C:\\temp\\vetPracticeCodes.csv')