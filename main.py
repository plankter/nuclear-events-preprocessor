import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="Nucleus")

def preprocess(df):
    df.dropna(axis=0, subset=['EventNumber'], inplace=True)
    df['EventNumber'] = df.EventNumber.astype(int)
    df['Industry'] = df.Industry.astype(dtype='category')
    df['Facility'] = df.Facility.astype(dtype='category')
    df['Mode'] = df.Mode.astype(dtype='category')
    df['Type'] = df.Type.astype(dtype='category')
    df['Status'] = df.Status.astype(dtype='category')

    dates = pd.to_datetime(dict(year=df.Year, month=df.Month, day=df.Day))
    df.insert(1, "Date", dates)
    df.drop(columns=['Year', 'Month', 'Day'], inplace=True)
    return df

def get_location(df):
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    locations = df.head()['Geographical Location'].apply(geocode)
    print(locations)
    #df['Latitude'] = locations.Latitude
    #df['Longitude'] = locations.Longitude
    return df


df = pd.read_excel('data/data.xlsx')  # sheetname is optional

df = preprocess(df)
df = get_location(df)

df.to_csv('data/data.csv', index=False)  # index=False prevents pandas to write row index
