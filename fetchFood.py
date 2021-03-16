from pymongo import MongoClient
import datetime
import pandas as pd
import re
from itertools import repeat
from bs4 import BeautifulSoup
from selenium import webdriver

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['fetchScraping']
products = db['foodProducts']
########################################################################

######################## KEY VARIABLES ########################
writeToDB = False
dateTimeOfScrape = datetime.datetime.utcnow()
docs = []
###############################################################


driver = webdriver.Chrome('C:\\python\\chromedriver')

driver.get('https://fetch.co.uk/dogs/dog-food/dry-dog-food?per-page=1000')
driver.execute_script('load()')
#html = driver.page_source
#soup = BeautifulSoup(html)


driver.quit()
