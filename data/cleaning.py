# SEARCHING FOR A CITY.

# extracting a list of the countries chosing the category of the business.
def music_countries (category_code, country_code):
    condition1 = {"category_code":category_code}
    projection = {"name":1, "_id":0, "offices.country_code":country_code}
    countries = (list(c.find(condition1, 
               projection)))
    # iterate to have the single elements (single name countries) in a list.
    listcountry = [countries[i]["offices"][j]["country_code"] for i in range(len(countries)) for j in range(len(countries[i]["offices"]))]
    return listcountry

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
    

 
 # Startups. ---------------------------------------

# filter information and transform it to df.





# export info to csv.
def export_SD_startups (df):
    return df.to_csv(f"D:\\ironhack\\proyectos\\GeoSpatialData_proy3\csv\\startups_sandiego.csv")   