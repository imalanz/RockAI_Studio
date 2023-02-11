# GRAPHS

# clean and filter data frame to do a graphic.
def clean_cities_df (df):
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.dropna(how="any", inplace=True)
    df.sort_values(by=['count'],ascending=False, inplace = True)
    # just get the top 10.
    x = df[:10]
    return x

#
