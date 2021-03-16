from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import re

##### Connect to MongoDB database#######################################
client = MongoClient('localhost')
db = client['petsScraping']
products = db['allProducts']
########################################################################

dfAll = pd.DataFrame.from_records(products.find())
totalProducts = str(len(dfAll))
dfAll[['category1','category2','category3','category4']] = pd.DataFrame(dfAll.categories.values.tolist(), index= dfAll.index)

category1 = dfAll.groupby('category1')['category1'].count()
print(category1)
print('')

category2 = dfAll.groupby('category2')['category2'].count()
print(category2)
print('')

category3 = dfAll.groupby('category3')['category3'].count()
print(category3)
print('')


print('')
print('Total products: ' + totalProducts)



