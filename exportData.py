from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import re
from analysis import applyInitialAdjustments, applyPromo

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['petsScraping']
products = db['foodProducts']
brands = db['foodBrands']
dbZooplus = client['zooplusScraping']
productsZooplus = dbZooplus['foodProducts']
dbAmazon = client['amazonScraping']
comparisons = dbAmazon['popularDryDogFood']
########################################################################

dfAll = pd.DataFrame.from_records(products.find())
dfBrands = pd.DataFrame.from_records(brands.find())['name'].tolist()
brandsDict = {}

dfAllZooplus = pd.DataFrame.from_records(productsZooplus.find())

for b in dfBrands:
    brandsDict[b.split(' ')[0].strip().lower().replace("'","")] = b.strip().lower()

dfAll['size'] = dfAll.apply(lambda row: applyInitialAdjustments(row['title'], row['size']), axis=1)
dfAll['pricePerKilo'] = dfAll['price'] / dfAll['size']
dfAll['pricePerKiloAfterPromo'] = dfAll.apply(lambda row: applyPromo(row['promo'], row['size'], row['price'], row['pricePerKilo']), axis=1)

dog = dfAll[dfAll['categories'].apply(lambda x: 'dog' in x)]
dogWithoutTreats = dog[dog['categories'].apply(lambda x: 'dog-treats' not in x)]
dryDogFood = dog[dog['categories'].apply(lambda x: 'dry-dog-food' in x)].copy()

dogZooplus = dfAllZooplus[dfAllZooplus['categories'].apply(lambda x: 'dogs' in x)]
dryDogFoodZooplus = dogZooplus[dogZooplus['categories'].apply(lambda x: 'dry_dog_food' in x)].copy()
dryDogFoodZooplus['description'] = dryDogFoodZooplus['description'].str.strip()
dryDogFoodZooplus['title'] = dryDogFoodZooplus['title'].str.strip()
dryDogFoodZooplus['nutrition'] = dryDogFoodZooplus['nutrition'].str.strip()
dryDogFoodZooplus['finalPrice'] = dryDogFoodZooplus['finalPrice'].str.strip().str.replace('£', '')
dryDogFoodZooplus['pricePerKilo'] = dryDogFoodZooplus['pricePerKilo'].str.replace('(', '').str.replace('/ kg', '').str.replace(')', '').str.strip().str.replace('£', '')
dryDogFoodZooplus['beforePromo'] = dryDogFoodZooplus['beforePromo'].str.split('£').str[1].str.strip()

dryDogFoodZooplus = dryDogFoodZooplus.drop('description', 1)
dryDogFoodZooplus.rename(columns={'title':'Description'}, inplace=True)
dryDogFoodZooplus['store'] = 'Zoo_Plus'
dryDogFoodZooplus.rename(columns={'store':'Store'}, inplace=True)
dryDogFoodZooplus['Brand'] = ''
dryDogFoodZooplus['Pet'] = 'Dog'
dryDogFoodZooplus['Type'] = 'Dry'
dryDogFoodZooplus['ID'] = ''
dryDogFoodZooplus['Date'] = dryDogFoodZooplus['dateTimeOfScrape'].dt.strftime('%d/%m/%Y')
dryDogFoodZooplus['Package Size'] = (pd.to_numeric(dryDogFoodZooplus['finalPrice'], errors='coerce') / pd.to_numeric(dryDogFoodZooplus['pricePerKilo'], errors='coerce')).round(1)
dryDogFoodZooplus.rename(columns={'finalPrice':'Store Price', 'pricePerKilo':'Price per KG'}, inplace=True)
dryDogFoodZooplus['Normal Price'] = np.where(dryDogFoodZooplus['beforePromo'].isnull(), dryDogFoodZooplus['Store Price'], dryDogFoodZooplus['beforePromo'])
dryDogFoodZooplus = dryDogFoodZooplus[['ID', 'Date', 'Store', 'Brand', 'Pet', 'Type', 'Description', 'Package Size', 'Store Price', 'Normal Price', 'Price per KG']]

dryDogFood['Brand'] = ''
dryDogFood['Pet'] = 'Dog'
dryDogFood['Type'] = 'Dry'
dryDogFood['ID'] = ''
dryDogFood['Store'] = 'Pets_at_Home'
dryDogFood['Normal Price'] = dryDogFood['price']
dryDogFood['Store Price'] = dryDogFood['pricePerKiloAfterPromo'] * dryDogFood['size']
dryDogFood['Date'] = dryDogFood['dateTimeOfScrape'].dt.strftime('%d/%m/%Y')
dryDogFood.rename(columns={'finalPrice':'Store Price', 'pricePerKiloAfterPromo':'Price per KG', 'size':'Package Size', 'title': 'Description'}, inplace=True)
dryDogFood = dryDogFood[['ID', 'Date', 'Store', 'Brand', 'Pet', 'Type', 'Description', 'Package Size', 'Store Price', 'Normal Price', 'Price per KG']]

result = dryDogFood.append(dryDogFoodZooplus)
result.to_excel('S:\\PORTFOLIO\\Invest\\Admin\\Database\\petsZooplus.xlsx', index=False)

#print(len(dryDogFoodZooplus))
#dryDogFoodZooplus.to_csv('C:\\temp\\zooplusF.csv')