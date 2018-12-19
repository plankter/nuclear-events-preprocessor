import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import psycopg2

geolocator = Nominatim(user_agent="Nucleus")


def preprocess_events(df):
    df.dropna(axis=0, subset=['EventNumber'], inplace=True)
    df['EventNumber'] = df.EventNumber.astype(int)
    df['Industry'] = df.Industry.astype(dtype='category')
    df['Facility'] = df.Facility.astype(dtype='category')
    df['Mode'] = df.Mode.astype(dtype='category')
    df['Type'] = df.Type.astype(dtype='category')
    df['Status'] = df.Status.astype(dtype='category')

    dates = pd.to_datetime(dict(year=df.Year, month=df.Month, day=df.Day))
    df.insert(1, "Date", dates)

    df.rename(columns={"EventNumber": "event_number", "Geographical Location": "location",
                       "INES (guess)": "ines_guess", "Gross Electrical Capacity [MW]": "capacity",
                       "Grid Connection Year": "connection_year", "Comments (DB V3)": "comments",
                       "Core relevant": "core_relevant", "Origin_description": "origin_description",
                       "Origin (incl. potential origin in the case of a degraded system)": "origin",
                       "Cause_description": "cause_description",
                       "Cause (incl. potential cause)": "cause", "Lower Limit": "lower_limit",
                       "Upper Limit": "upper_limit"}, inplace=True)
    df.columns = df.columns.str.lower()
    return df


def preprocess_facilities(df):
    df.drop(axis=0, columns=['Date'], inplace=True)
    df.dropna(axis=0, subset=['EventNumber'], inplace=True)
    df['EventNumber'] = df.EventNumber.astype(int)
    df['Industry'] = df.Industry.astype(dtype='category')
    df['Facility'] = df.Facility.astype(dtype='category')

    dates = pd.to_datetime(dict(year=df.Year, month=df.Month, day=df.Day))
    df.insert(1, "Date", dates)

    df.rename(columns={"EventNumber": "event_number", "Geographical Location": "location",
                       "INES (guess)": "ines_guess"}, inplace=True)
    df.columns = df.columns.str.lower()
    return df


def preprocess_references(df):
    df.dropna(axis=0, subset=['EventNumber'], inplace=True)
    df['EventNumber'] = df.EventNumber.astype(int)

    df.rename(columns={"EventNumber": "event_number"}, inplace=True)
    df.columns = df.columns.str.lower()
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


# events_df = pd.read_excel('data/data.xlsx')  # sheetname is optional
# events_df = preprocess_events(events_df)
# events_df = get_location(events_df)
#
# events_df.to_pickle('data/events.pkl')
# events_df.to_csv('data/events.csv', index=False, sep='\t')  # index=False prevents pandas to write row index
# events_df.to_json('data/events.json', date_format='iso')


# facilities_df = pd.read_excel('data/otherFacilities.xlsx')  # sheetname is optional
# facilities_df = preprocess_facilities(facilities_df)
# facilities_df = get_location(facilities_df)
#
# facilities_df.to_pickle('data/facilities.pkl')
# facilities_df.to_csv('data/facilities.csv', index=False, sep='\t')  # index=False prevents pandas to write row index
# facilities_df.to_json('data/facilities.json', date_format='iso')

references_df = pd.read_excel('data/eventIdReferences.xlsx')
references_df = preprocess_references(references_df)
conn = psycopg2.connect("host=localhost dbname=iwtoolbox user=admin password=innoadmin")
cur = conn.cursor()

sql = """ UPDATE nuclear_event
                SET "references" = %s
                WHERE event_number = %s"""
for _, row in references_df.iterrows():
    cur.execute(sql, (row.references, row.event_number))

conn.commit()
cur.close()
conn.close()

