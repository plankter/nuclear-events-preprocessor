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

    df['Year'] = df.EventNumber.astype(int)
    df['Month'] = df.EventNumber.astype(int)
    df['Day'] = df.EventNumber.astype(int)

    df.rename(columns={"EventNumber": "event_number", "Geographical Location": "location",
                       "INES (guess)": "ines_guess", "Gross Electrical Capacity [MW]": "capacity",
                       "Grid Connection Year": "connection_year", "Comments (DB V3)": "comments",
                       "Core relevant": "core_relevant", "Origin_description": "origin_description",
                       "Origin (incl. potential origin in the case of a degraded system)": "origin",
                       "Cause_description": "cause_description",
                       "Cause (incl. potential cause)": "cause", "Lower Limit": "lower_limit",
                       "Upper Limit": "upper_limit"}, inplace=True)
    df.rename(str.lower, axis='columns')
    return df


def get_location(df):
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.1)
    latitude = []
    longitude = []
    for row in df.location:
        location = geocode(row)
        if location is not None:
            latitude.append(location.latitude)
            longitude.append(location.longitude)
        else:
            latitude.append(None)
            longitude.append(None)

    df.insert(6, "latitude", latitude)
    df.insert(7, "longitude", longitude)
    return df


df = pd.read_excel('data/data.xlsx')  # sheetname is optional

df = preprocess(df)
df = get_location(df)

df.to_pickle('data/output.pkl')
df.to_csv('data/output.csv', index=False, sep='\t', escapechar='\\')  # index=False prevents pandas to write row index
df.to_json('data/output.json', date_format='iso')
