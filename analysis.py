from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import re

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

def getCleanedZooplusDryDogFood():
    rawData = pd.DataFrame.from_records(productsZooplus.find())
    rawData = rawData[rawData['categories'].apply(lambda x: 'dry_dog_food' in x)].copy()
    rawData['altTitle'] = rawData['altTitle'].str.strip()
    rawData['title'] = rawData['title'].str.strip()
    return rawData

def getUniquePromos(df):
    promos = set()
    def addToSet(inputList):
        for l in inputList:
            promos.add(l)

    df['promo'].apply(addToSet)
    y = list(promos)
    print(sorted(y))
    havePromo = df[df['promo'].apply(lambda x: 'Price Cut - Save £3.99' in x)]
    print(havePromo)

def round_of_rating(number):
    return round(number * 2) / 2

def addSize(size, text):
    if 'kg' not in text.lower():
        return text + ' ' + str('%g'%(size))
    return text

def applyInitialAdjustments(title, size):
    if 'rehydrated' in title.lower():
        try:
            rehydratedSize = re.search('\(([^)]+)', title.lower()).group(1)
            rehydratedSize = re.search(r'(.*?)kg', rehydratedSize)
            return float(rehydratedSize.group(1))
        except:
            rehydratedSize = re.search('\(([^)]+)', title.lower()).group(1)
            rehydratedSize = re.search(r'(.*?)g', rehydratedSize)
            return (float(rehydratedSize.group(1))/1000)
    else:
        return size

def applyPromo(promo, size, price, pricePerKilo):
    for p in promo:
        p = p.lower().strip()

        if 'when you spend' in p and 'on selected' in p:
            return pricePerKilo

        if ('sticks' in p or 'pack' in p) and 'on' in p:
            return pricePerKilo

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

#print(dfAll.iloc[23167])
'''
dfAll['size'] = dfAll.apply(lambda row: applyInitialAdjustments(row['title'], row['size']), axis=1)
dfAll['pricePerKilo'] = dfAll['price'] / dfAll['size']
dfAll['pricePerKiloAfterPromo'] = dfAll.apply(lambda row: applyPromo(row['promo'], row['size'], row['price'], row['pricePerKilo']), axis=1)


dogZooplus = dfAllZooplus[dfAllZooplus['categories'].apply(lambda x: 'dogs' in x)]
dryDogFoodZooplus = dogZooplus[dogZooplus['categories'].apply(lambda x: 'dry_dog_food' in x)].copy()
dryDogFoodZooplus['description'] = dryDogFoodZooplus['description'].str.strip()
dryDogFoodZooplus['title'] = dryDogFoodZooplus['title'].str.strip()
dryDogFoodZooplus['nutrition'] = dryDogFoodZooplus['nutrition'].str.strip()
dryDogFoodZooplus['finalPrice'] = dryDogFoodZooplus['finalPrice'].str.strip().str.replace('£', '')
dryDogFoodZooplus['pricePerKilo'] = dryDogFoodZooplus['pricePerKilo'].str.replace('(', '').str.replace('/ kg', '').str.replace(')', '').str.strip().str.replace('£', '')
dryDogFoodZooplus['beforePromo'] = dryDogFoodZooplus['beforePromo'].str.split('£').str[1].str.strip()
print(dryDogFoodZooplus)

print(len(dryDogFoodZooplus))
dryDogFoodZooplus.to_csv('C:\\temp\\zooplusF.csv')


dryDogFoodLast = None
for name, df in dfAll.groupby('dateTimeOfScrape'):
    dog = df[df['categories'].apply(lambda x: 'dog' in x)]
    dogWithoutTreats = dog[dog['categories'].apply(lambda x: 'dog-treats' not in x)]
    dryDogFood = dog[dog['categories'].apply(lambda x: 'dry-dog-food' in x)].copy()
    cat = df[df['categories'].apply(lambda x: 'cat' in x)]
    catWithoutTreats = cat[cat['categories'].apply(lambda x: 'cat-treats' not in x)]
    dryCatFood = cat[cat['categories'].apply(lambda x: 'dry-cat-food' in x)]

    dryDogFood['brand'] = dryDogFood.title.str.split().str.get(0)
    dryDogFood['brand'] = dryDogFood['brand'].str.lower().str.replace("'","")
    dryDogFood['brand'] = dryDogFood['brand'].map(brandsDict)
    dryDogFoodLast = dryDogFood

    #test = dryDogFood[dryDogFood['brand'] == 'royal canin'].copy()
    #dryDogFood['unique'] = dryDogFood['productCode'].map(str) + dryDogFood['size'].map(str)
    #dryDogFood = dryDogFood.drop_duplicates('unique')
    #print(dryDogFood.groupby('brand')['brand'].count())
    #dryDogFood.to_csv('C:\\temp\\dryDogFood3.csv')

    print(name)
    print('Number of items scraped: ' + str(len(dryDogFood)))
    print('Dry dog food (without promos): ' + str(dryDogFood['pricePerKilo'].mean()))
    print('All dog food (without promos): ' + str(dogWithoutTreats['pricePerKilo'].mean()))
    print('Price per kilo (with promos):')
    print('Dry dog food: ' + str(dryDogFood['pricePerKiloAfterPromo'].mean()))
    print('Dry cat food: ' + str(dryCatFood['pricePerKiloAfterPromo'].mean()))
    print('All dog food: ' + str(dogWithoutTreats['pricePerKiloAfterPromo'].mean()))
    print('All cat food: ' + str(catWithoutTreats['pricePerKiloAfterPromo'].mean()))
    print('')


amazonPopular = pd.DataFrame.from_records(comparisons.find())
amazonPopular['size'] = (amazonPopular['price'] / amazonPopular['pricePerKilo']).apply(round_of_rating)
amazonPopular = amazonPopular[amazonPopular['productCode'] != '']
amazonPopular['productCode'] = amazonPopular.apply(lambda row: addSize(row['size'], row['productCode']), axis=1)
dryDogFoodLast['productCode'] = dryDogFoodLast.apply(lambda row: addSize(row['size'], row['productCode']), axis=1)
dryDogFoodLast.rename(columns={'pricePerKilo':'pricePerKiloPets'}, inplace=True)
amazonComp = pd.merge(amazonPopular, dryDogFoodLast[['productCode', 'pricePerKiloAfterPromo', 'pricePerKiloPets', 'brand']], on='productCode')
amazonComp['differenceWithDiscount'] = amazonComp['pricePerKiloAfterPromo'] - amazonComp['pricePerKilo']
amazonComp['differenceWithoutDiscount'] = amazonComp['pricePerKiloPets'] - amazonComp['pricePerKilo']
print(amazonComp['differenceWithoutDiscount'].mean())
print(amazonComp['differenceWithDiscount'].mean())
#print(amazonComp.groupby('brand')['differenceWithDiscount'].mean())
#print(amazonComp['size'].mean())
#print(dryDogFoodLast['size'].mean())
'''
'''
promoNonIntroductory = havePromo[~havePromo['promo'].str.contains('Introductory Offer')]

priceCut = dryDogFood[dryDogFood['promo'].str.contains('Price Cut')]

ex = dryDogFood[dryDogFood['promo']=='Online - Now £18, Save £2.99']
print(promos)'''


###########################################################
'''
avPricePerKilo = dogWithoutTreats[['pricePerKilo']].mean()
bins = [0,2,4,6,8,10]
plt.hist(dryDogFood['pricePerKilo'], bins, alpha = 0.4)
plt.hist(dryDogFood['pricePerKiloAfterPromo'], bins, alpha = 0.6)
#x = dryDogFood['pricePerKilo']#np.random.normal(size = 1000)
#plt.hist(x, bins=20)
plt.show()'''

#print(getCleanedZooplusDryDogFood())