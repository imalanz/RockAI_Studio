
# EXPLORATION.

from pymongo import MongoClient
import pandas as pd
import time
import importlib
import geopandas as gpd
import json
from cartoframes.viz import Map, Layer, popup_element
from getpass import getpass
import os
import requests
from dotenv import load_dotenv
import pymongo

# Mongo.
client = MongoClient("localhost:27017")
db = client["Ironhack"]
c = db.get_collection("companies")
nyneights = db.get_collection("nyneigh")
mongo_starbucks = db.get_collection("ny_starbucks")

# 1.2. country. ---------------
# extracting a list of the countries chosing the category of the business.
def music_countries (category_code, country_code):
    condition1 = {"category_code":category_code}
    projection = {"name":1, "_id":0, "offices.country_code":country_code}
    countries = (list(c.find(condition1, 
               projection)))
    # iterate to have the single elements (single name countries) in a list.
    listcountry = [countries[i]["offices"][j]["country_code"] for i in range(len(countries)) for j in range(len(countries[i]["offices"]))]
    return listcountry

# 1.3. City. ---------------------
# Extracting from mongo list of cities chosing the category of the business.
def music_cities (category_code, cities):
    condition1 = {"category_code":category_code}
    projection = {"name":1, "_id":0, "offices.city":cities}
    city = (list(c.find(condition1, 
               projection)))
    # Iteration to throw a list of just the name of the cities.
    lstcity = [city[i]["offices"][j]["city"] for i in range(len(city)) for j in range(len(city[i]["offices"]))]
    return lstcity

# Merge the 2 lists into one, and throw a data frame.
def combine_lists (lst1, lst2):
    # change the 2 lists into a dictionary
    location = {"cities": lst1,
            "country": lst2} 
    # create them a Data Frame.
    location = pd.DataFrame(location)
    # groupby the column cities and create a new column with the value_counts of cities.
    location["count"] = location.groupby(["cities"])["country"].transform("count")
    return location

# Clean the data frame, from dulicates and nulls.
def clean_df_citycountry (df):
    x = df.combine_first(df).reset_index().reindex(df.columns, axis=1).drop_duplicates()
    x.dropna(how="any", inplace=True)
    return x

# export to csv file - need to change name of file to save.
def export_location (df):
    return df.to_csv("D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\location_clean.csv")
    

# 2. Music Companies NY. ----------------------

# Filtering from mongo name, latitude and longitud. to a dictionary.
def music_NY (category_code, cities):
    condition1 = {"category_code":category_code}
    condition2 = {"offices.city":cities}
    projection = {"name":1, "_id":0,"offices.latitude":1, "offices.longitude":1}
    city = (list(c.find({"$and":[condition1, condition2]}, 
               projection)))
    return city

# selecting only the lat.
def lat_ (dict):
    x = []
    for i in range(len(dict)):
        x.append(dict[i]["offices"][0]["latitude"])
    return x

# selecting only the long.
def long_ (dict):
    y = []
    for i in range(len(dict)):
        y.append(dict[i]["offices"][0]["longitude"])
    return y

# dictionary for latitud and long, to create df.
def coord_df (lats, longs):
    lon_lat = {"latitude": lats,
              "longitude": longs}
    df = pd.DataFrame(lon_lat)
    return df

# Filter again the music companies but without the latitud and logitude and turn it into a data frame.
def musicdf_NY (category_code, city):
    # Conditions for city and amount of the company.
    condition1 = {"category_code":category_code}
    condition2 = {"offices.city":city}
    projection = {"name":1, "_id":0}
    x = (list(c.find({"$and":
                             [condition1, condition2]},
                   projection)))
    # give a Data frame with conditions and address.
    df = pd.DataFrame(x)
    return df    

# concat the 3 df.
def concat_axis1 (df1, df2):
    starts = pd.concat([df1, df2], axis=1)
    return starts

# For the geo intersection, from lat and lon throw a list of the neighborhood.
def dict_format_point (lat, lon):
# Iterate throw lists of only lon and lat. with the point format to intersect. 
    d = []
    for i in range(len(lat)):
        d.append({"type": "Point", "coordinates": [lon[i], lat[i]]})
    # get the list of the neightboors of the intersection/ throws a nested list.
    w = []
    for i in d:
        x = list(nyneights.find(
        {"geometry":
            {"$geoIntersects": 
            {"$geometry": i}}}, projection = {"name": 1, "_id":0}))
        w.append(x) 
    # comprehention list for get just a list with dictionaries.       
    comp = [j for i in w for j in i]
    return comp

# Exporting to csv.
def export_NY_music (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\musiccompanies_newyork.csv")


# 3. Startups. ---------------------------------------

# filter information of city and amount of money.
def startups_cooord (city, amount):
    # Conditions for city and amount of the company.
    condition1 = {"offices.city":city}
    condition2 = {"acquisitions.price_amount":{"$gte":amount}}
    projection = {"name":1, "_id":0, "category_code":1, "offices.latitude":1, "offices.longitude":1}
    x = (list(c.find({"$and":
                             [condition1, condition2]},
                   projection)))
    return x

# Filter again the startups but without the latitud and logitude and turn it into a data frame.
def startups_name (city, amount):
    # Conditions for city and amount of the company.
    condition1 = {"offices.city":city}
    condition2 = {"acquisitions.price_amount":{"$gte":amount}}
    projection = {"name":1, "_id":0, "category_code":1, "acquisitions.price_amount":1}
    x = (list(c.find({"$and":
                             [condition1, condition2]},
                   projection)))
    # give a Data frame with conditions and address.
    df = pd.DataFrame(x)
    return df  

# function to separete the price amount of startup table and put it a data frame.
def price_amount (city, amount):
    condition1 = {"offices.city": city}
    condition2 = {"acquisitions.price_amount":{"$gte":amount}}
    projection = {"name":1, "_id":0, "category_code":1, "acquisitions.price_amount":1}
    x = (list(c.find({"$and":
                                [condition1, condition2]},
                    projection)))
        #iterate the dict to get the price amount.
    y=[]
    for i in range(len(x)):
        y.append(x[i]["acquisitions"][0]["price_amount"])
        # give a Data frame with amount.
    df = pd.DataFrame(y)
    return df

# dictionary for latitud and long, to create df.
def coord_df (lats, longs):
    lon_lat = {"latitud": lats,
              "longitude": longs}
    df = pd.DataFrame(lon_lat)
    return df

# used concat function above.

# Clean the list of non values. convert to data frame, delete nulls and back again to list.
def delete_null (lst):
    startups_lat = pd.DataFrame(lst)
    startups_lat.dropna(how="any", inplace=True)
    startups_lat = startups_lat.values.tolist()
    lat_lst = [j for i in startups_lat for j in i]
    return lat_lst

# export to csv.
def export_NY_startups (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\startups_newyork.csv")


# 4. Foursquare. ------------------------------------------------

# get the foursquare list of each type of searching.
def foursquare (tipo, category):
    token = getpass()
    url = "https://api.foursquare.com/v3/places/search"

    params = {
        "query": tipo,
        "ll": "40.7380216,-74.003227113",
        "open_now": "true",
        "sort":"DISTANCE",
        "radius": 15000,
        "limit": 50,
        "category": category
    }

    headers = {
        "Accept": "application/json",
        "Authorization": token
    }

    response = requests.request("GET", url, params=params, headers=headers)

    return response.json()["results"]

# Get only the name, latitude, longitude of the hole dict. need to look it single [0].
def single_coord_name (dict_):
    dicts = {"name": dict_["name"],
            "latitude": dict_["geocodes"]["main"]["latitude"],
             "longitude": dict_["geocodes"]["main"]["longitude"]}  
    return dicts

# Iterate the full list of foursquare and append the single function (single_coord_name), to iterate just what the single function gets.
def iterate_all (full_dict_):
    lst = []
    for i in full_dict_:
        lst.append(single_coord_name(i))
    return lst

# make it a data frame.
def make_df (dict_):
    return pd.DataFrame(dict_)

# geointersection = make a new data frame from exporting the DF to a mongo file, make the points of lat and long to intersecto to geojson neights.
def geointersection_dict (mongo_):
    # get info of latitude and longitud of collection created. 
    coords = list(mongo_.find({},{"_id":0, "latitude":1, "longitude":1} ))
    # get latitude list.
    latitude = []
    for i in range(len(coords)):
        latitude.append(coords[i]["latitude"])
    # get longitude list.
    longitude = []
    for i in range(len(coords)):
        longitude.append(coords[i]["longitude"])
    # put them tougether in a single list with the format for getting the geointersection {"type": "Point", "coordinates": [longitude[i], latitude[i]]}
    d = []
    for i in range(len(latitude)):
        d.append({"type": "Point", "coordinates": [longitude[i], latitude[i]]})
    # get the list of the neightboors of the intersection/ throws a nested list.
    w = []
    for i in d:
        x = list(nyneights.find(
        {"geometry":
            {"$geoIntersects": 
            {"$geometry": i}}}, projection = {"name": 1, "_id":0}))
        w.append(x)    
    # comprehention list for get just a list with dictionaries.       
    comp = [j for i in w for j in i]
    return comp

# Clean data frame and create new columns name="string", porcentage=integer.
def new_columns (df, name, porcentage):  
    df["count"] = df.groupby(["name"])["name"].transform("count")
    stbks = df.drop_duplicates(['name','count'],keep= 'last')
    stbks[name] = stbks["count"]*porcentage
    return stbks

# export df.
def export_NY_starbucks (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\starbucks_newyork.csv")


# export to csv.
def export_NY_schools (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\schools_newyork.csv")


# export to csv.
def export_NY_bars (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\bars_newyork.csv")


# export to csv.
def export_NY_concerts (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\concerts_newyork.csv")

# Merge all count tables and clean it to a big df.
def merge_count_tables (df1, df2, df3, df4, df5, df6):
    a = pd.merge(df1, df2, on='name', how='outer')
    b = pd.merge(a, df3, on='name', how='outer')
    c = pd.merge(b, df4, on='name', how='outer')
    d = pd.merge(c, df5, on='name', how='outer')
    e = pd.merge(d, df6, on='name', how='outer')
    e.drop(columns=["count_x", "count_y"], inplace=True)
    e.rename(columns={'name': 'neighbourhood'}, inplace=True, errors='raise')
    return e

# export to csv.
def export_NY_neigh (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\neigh_newyork.csv")