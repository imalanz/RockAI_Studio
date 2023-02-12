
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
def lat_music (dict):
    x = []
    for i in range(len(music_ny)):
        x.append(music_ny[i]["offices"][0]["latitude"])
    return x

# selecting only the long.
def long_music (dict):
    y = []
    for i in range(len(music_ny)):
        y.append(music_ny[i]["offices"][0]["longitude"])
    return y

# dictionary for latitud and long, to create df.
def coord_df (lats, longs):
    lon_lat = {"latitud": lats,
              "longitude": longs}
    df = pd.DataFrame(lon_lat)
    return df

# Filter again the music companies but without the latitud and logitude and turn it into a data frame.
def music_NY (category_code, city):
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

# concat the 2 df.
def concat_axis1 (df1, df2):
    starts = pd.concat([df1, df2], axis=1)
    return starts

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

# Get latitude in list.
def lat_startup (dict):
    x = []
    for i in range(len(startups)):
        x.append(startups[i]["offices"][0]["latitude"])
    return x

# get longitud in list.
def long_startup (dict):
    y = []
    for i in range(len(startups)):
        y.append(startups[i]["offices"][0]["longitude"])
    return y

# Filter again the startups but without the latitud and logitude and turn it into a data frame.
def startups_name (city, amount):
    # Conditions for city and amount of the company.
    condition1 = {"offices.city":city}
    condition2 = {"acquisitions.price_amount":{"$gte":amount}}
    projection = {"name":1, "_id":0, "category_code":1}
    x = (list(c.find({"$and":
                             [condition1, condition2]},
                   projection)))
    # give a Data frame with conditions and address.
    df = pd.DataFrame(x)
    return df  

# dictionary for latitud and long, to create df.
def coord_df (lats, longs):
    lon_lat = {"latitud": lats,
              "longitude": longs}
    df = pd.DataFrame(lon_lat)
    return df

# used concat function above.

# export to csv.
def export_NY_startups (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\startups_newyork.csv")


# 4. Foursquare.

# 4.1. Starbucks.

# Make a dictionary for a single index.
def coord_name (dict_):
    dicts = {"name": dict_["name"],
            "latitude": dict_["geocodes"]["main"]["latitude"],
             "longitude": dict_["geocodes"]["main"]["longitude"]}  
    return dicts

def export_NY_starbucks (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\starbucks_newyork.csv")

# 4.2. Schools.

# export to csv.
def export_NY_schools (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\schools_newyork.csv")

# 4.3. Bars.

# export to csv.
def export_NY_bars (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\bars_newyork.csv")


