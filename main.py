import pandas as pd
from datetime import date, timedelta 
import urllib.request
from io import open
from io import StringIO
from geopy.distance import distance, geodesic
import numpy as np
import time
import math
import matplotlib.pyplot as plt

yst = "26.5"
yen = "41.5"
xst = "129"
xen = "143"
author = "JMA"
type_ = "Any"



def date_to_jd(year,month,day):
    if month == 1 or month == 2:
        yearp = year - 1
        monthp = month + 12
    else:
        yearp = year
        monthp = month
    if ((year < 1582) or
        (year == 1582 and month < 10) or
        (year == 1582 and month == 10 and day < 15)):
        B = 0
    else:
        A = math.trunc(yearp / 100.)
        B = 2 - A + math.trunc(A / 4.)
    if yearp < 0:
        C = math.trunc((365.25 * yearp) - 0.75)
    else:
        C = math.trunc(365.25 * yearp)
    D = math.trunc(30.6001 * (monthp + 1))
    jd = B + C + D + day + 1720994.5
    return jd

def jd_to_date(jd):
    jd = jd + 0.5
    F, I = math.modf(jd)
    I = int(I)
    A = math.trunc((I - 1867216.25)/36524.25)
    if I > 2299160:
        B = I + 1 + A - math.trunc(A / 4.)
    else:
        B = I   
    C = B + 1524
    D = math.trunc((C - 122.1) / 365.25)
    E = math.trunc(365.25 * D)
    G = math.trunc((C - E) / 30.6001)
    day = C - E + F - math.trunc(30.6001 * G)
    if G < 13.5:
        month = G - 1
    else:
        month = G - 13
        
    if month > 2.5:
        year = D - 4716
    else:
        year = D - 4715
        
    return date(int(year), int(month), int(day))

def load_data():

    yst = "26.5"
    yen = "41.5"
    xst = "129"
    xen = "143"
    author = "JMA"
    type_ = "Any"

    #В качестве обучающей и валидационной выборки будем брать период с 1995 по 2005

    start_time = date(1995, 1, 1)
    end_time = date(1995, 4, 1)

    #будем загружать периодами по 3 месяца

    url = "http://isc-mirror.iris.washington.edu/cgi-bin/web-db-v4?request=COMPREHENSIVE&out_format=CATCSV&searchshape=RECT&bot_lat=" + yst + "&top_lat=" + yen + "&left_lon=" + xst + "&right_lon=" + xen + "&ctr_lat=&ctr_lon=&radius=&max_dist_units=deg&srn=&grn=&start_year=" + str(start_time.year) + "&start_month=" + str(start_time.month) + "&start_day=" + str(start_time.day) + "&start_time=00%3A00%3A00&end_year=" + str(end_time.year) + "&end_month=" + str(end_time.month) + "&end_day=" + str(end_time.day) + "&end_time=00%3A00%3A00&min_dep=&max_dep=&null_dep=on&min_mag=" + "&max_mag=&req_mag_type=" + type_ + "&req_mag_agcy=" + author
    csv = urllib.request.urlopen(url).read().decode("utf-8") 
    while csv.find("Please try again in a few minutes.") != -1:
        time.sleep(10)
        csv = urllib.request.urlopen(url).read().decode("utf-8") 
    start = csv.find('...')
    end = csv.find('STOP', start)
    csv = csv[start + 5:end]
    df = pd.read_csv(StringIO(csv), delimiter=",",  on_bad_lines='skip', index_col=False)

    #переименуем столбцы для удобства, чтобы в них не было лишних пробелов

    n = df.columns.size
    cols = dict()
    for i in range(0, n):
        cols[df.columns[i]] = df.columns[i].replace(' ', '')
    df = df.rename(columns = cols)

    #на всякий случай еще сохраним данные теми кусками, что скачиваются

    df_by_years = [df]

    #циклом скачаем данные за весь желаемый период и будем добавлять их в новую таблицу

    year = 1995
    month = 4
    while year < 2005:
        start_time = date(year, month, 1)
        if month == 10:
            end_time = date(year + 1, 1, 1)
        else:
            end_time = date(year, month + 3, 1)
        url = "http://isc-mirror.iris.washington.edu/cgi-bin/web-db-v4?request=COMPREHENSIVE&out_format=CATCSV&searchshape=RECT&bot_lat=" + yst + "&top_lat=" + yen + "&left_lon=" + xst + "&right_lon=" + xen + "&ctr_lat=&ctr_lon=&radius=&max_dist_units=deg&srn=&grn=&start_year=" + str(start_time.year) + "&start_month=" + str(start_time.month) + "&start_day=" + str(start_time.day) + "&start_time=00%3A00%3A00&end_year=" + str(end_time.year) + "&end_month=" + str(end_time.month) + "&end_day=" + str(end_time.day) + "&end_time=00%3A00%3A00&min_dep=&max_dep=&null_dep=on&min_mag=" + "&max_mag=&req_mag_type=" + type_ + "&req_mag_agcy=" + author

        st = time.time()
        csv = urllib.request.urlopen(url).read().decode("utf-8") 
        while csv.find("Please try again in a few minutes.") != -1:
            time.sleep(5)
            csv = urllib.request.urlopen(url).read().decode("utf-8")
        if csv.find("No events were found.") == -1:
            start = csv.find('...')
            end = csv.find('STOP', start)
            csv = csv[start + 5:end]
            df_next = pd.read_csv(StringIO(csv), delimiter=",",  on_bad_lines='skip', index_col=False)
            df_next = df_next.rename(columns = cols)
            df_by_years.append(df_next)
            df = pd.concat([df, df_next])
            print(str(year) + '-' + str(month) + ": " + str(round(time.time() - st, 3)) + " sec")
            month = (month + 2) % 12 + 1
            if month == 1:
                year += 1

    df.to_csv('raw_data.csv', index=False)

def histogramm(df):
    df['DATE'] = df.apply(lambda x: date(int(x['DATE'][:4]), int(x['DATE'][5:7]), int(x['DATE'][8:10])), axis=1)

    #Выведем гистограммы магнитуды с точностью до целой части 

    mx = math.ceil(df['MAG'].max())
    mn = int(df['MAG'].min())   
    plt.hist(df['MAG'], np.arange(mn, mx + 1, 1))
    plt.grid(True)
    plt.xlabel('Magnitude')
    plt.ylabel('#events')
    plt.show()

    for i in range(mn, mx + 1):
        m = df[df['MAG'].astype('int') == i].count()[0]
        print(str(i) + '-' + str(i + 1) + ': ' + str(m))

def info(df):
    df['DATE'] = df.apply(lambda x: date(int(x['DATE'][:4]), int(x['DATE'][5:7]), int(x['DATE'][8:10])), axis=1)

    #найдем минимальные и максимальные значения координат, дат и магнитуд

    statistics = df[['DATE', 'LAT', 'LON', 'MAG']].min().to_frame(name='min')
    statistics['max'] = df[['DATE', 'LAT', 'LON', 'MAG']].max()

    #найдем средние значения координат и магнитуд

    a = df[['LAT', 'LON', 'MAG']].mean()

    #чтобы найти средние значения дат, сначала переведем даты в юлианские дни, а потом обратно

    df['JD'] = df.apply(lambda x: date_to_jd(x['DATE'].year, x['DATE'].month, x['DATE'].day), axis=1)
    mean_JD = df['JD'].mean()
    a['DATE'] = jd_to_date(mean_JD)
    statistics['mean'] = a
    print(statistics)

def density(x, df, R = 100, T = 365):
    R_earth = 6371
    y_mean = (yst + yen) / 2
    dy = (R * 360) / (2 * np.pi * R_earth)
    dx = (R * 360) / (2 * np.pi * R_earth * np.cos(np.deg2rad(y_mean)))
    
    suit = [False] * df.shape[0]
    
    start_date = x['DATE']
    i = df[df['EVENTID'] == x['EVENTID']].index[0]
    while i >= 0 and (x['DATE'] - df.iloc[i]['DATE']) < timedelta(days = T):
        if abs(df.iloc[i]['LON'] - x['LON']) <= dx and abs(df.iloc[i]['LAT'] - x['LAT'] <= dy):
            if geodesic(x['COORDINATE'], df.iloc[i]['COORDINATE']).km < R and (x['DATE'] - df.iloc[i]['DATE']) < timedelta(days = T):
                suit[i] = True
        i -= 1
    
    n = suit.count(True)
    density = n / (np.pi * R ** 2 * T)
    return density

def fill_density(df):
    df['DATE'] = df.apply(lambda x: date(int(x['DATE'][:4]), int(x['DATE'][5:7]), int(x['DATE'][8:10])), axis=1)
    df['COORDINATE'] = df.apply(lambda x: (x['LAT'], x['LON']), axis = 1)
    df['DENSITY'] = df.apply(lambda x: density(x, df), axis = 1)
    return df

def mean_mag(x, df, R = 200, T = 700):
    R_earth = 6371
    y_mean = (yst + yen) / 2
    dy = (R * 360) / (2 * np.pi * R_earth)
    dx = (R * 360) / (2 * np.pi * R_earth * np.cos(np.deg2rad(y_mean)))
    suit = [False] * df.shape[0]
    for i in range(0, df.shape[0]):
        if abs(df.iloc[i]['LON'] - x['LON']) <= dx and abs(df.iloc[i]['LAT'] - x['LAT'] <= dy):
            if distance(x['COORDINATE'], df.iloc[i]['COORDINATE']).km < R and (x['DATE'] - df.iloc[i]['DATE']) < timedelta(days = T):
                suit[i] = True
    res = suit * df['MAG']
    a = res.to_list()
    m = res.sum() / (len(a) - a.count(0))
    return m

def fill_mean_mag(df):
    df['DATE'] = df.apply(lambda x: date(int(x['DATE'][:4]), int(x['DATE'][5:7]), int(x['DATE'][8:10])), axis=1)
    df['COORDINATE'] = df.apply(lambda x: (x['LAT'], x['LON']), axis = 1) 
    df['MEAN MAG'] = df.apply(lambda x: mean_mag(x, df), axis = 1)
    return df

if __name__ == '__main__':
    load_data()

    #чтобы не ждать снова загрузки, можно из файла

    df = pd.read_csv('raw_data.csv')

    histogramm(df)
    info(df)
    df = fill_density(df)
    df = fill_mean_mag(df)
    df.to_csv('data_edit_1.csv', index=False)
