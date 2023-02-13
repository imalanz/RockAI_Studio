
# GRAPHS

import geopandas as gpd
from cartoframes.auth import set_default_credentials
from cartoframes.viz import Map, Layer, popup_element, default_legend, animation_style, basic_style,basic_legend, color_category_style, color_category_legend
set_default_credentials('cartovl')
set_default_credentials('cartoframes')



# clean and filter data frame to do a graphic.
def clean_cities_df (df):
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.dropna(how="any", inplace=True)
    df.sort_values(by=['count'],ascending=False, inplace = True)
    # just get the top 10.
    x = df[:10]
    return x

# get the coordinates for carto maps.
def carto_points (df):
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["longitude"], df["latitud"]))

# Filter on carto maps for music df.
def carto_music (df):
    x = Map(Layer(df, basic_style(size=15, opacity=10, color="#008080"),legends=default_legend('music companies')))
    return x

# carto maps for startups.
def carto_startups (df):
    x = Map(Layer(df, basic_style(size=15, color="#b4c8a8"), legends=default_legend("Startups")))
    return x

# get the coordinates for carto maps in starbucks.
def starbucks_carto_points (df):
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["longitude"], df["latitude"]))
# carto maps for starbucks.
def carto_starbucks (df):
    x = Map(Layer(df, basic_style(size=15, color="#70a494"), legends=default_legend("Starbucks")))
    return x

# carto maps for schools.
def carto_schools (df):
    x = Map(Layer(df, basic_style(size=15, color="#f6edbd"), legends=default_legend("Schools")))
    return x

# carto maps for bars.
def carto_bars (df):
    x = Map(Layer(df, basic_style(size=15, color="#edbb8a"), legends=default_legend("Bars")))
    return x

# carto maps for concerts.
def carto_concerts (df):
    x = Map(Layer(df, basic_style(size=15, color="#de8a5a"), legends=default_legend("Concerts")))
    return x

# carto maps multiple layers.
def carto_layers (df1, df2, df3, df4, df5, df6):
    from palettable.cartocolors.diverging import Geyser_7
    x = Map([Layer(df1, basic_style(size=15, opacity=10, color="#008080"),legends=default_legend('music')),
        Layer(df2, basic_style(size=15, opacity=10, color="#70a494"), legends=default_legend("starbucks")),
        Layer(df3, basic_style(size=15, opacity=10, color="#b4c8a8"), legends=default_legend("Startups")),
        Layer(df4, basic_style(size=15, opacity=10, color="#f6edbd"), legends=default_legend("schools")),
        Layer(df5, basic_style(size=15, opacity=10, color="#edbb8a"), legends=default_legend("bars")),
        Layer(df5, basic_style(size=15, opacity=10, color="#de8a5a"), legends=default_legend("concerts"))], layer_selector=True)
    return x

