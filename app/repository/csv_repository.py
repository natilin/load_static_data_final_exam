import pandas as pd



def load_csv_type_1(csv_path) -> pd.DataFrame:
    cols = ['iyear',
         'imonth',
         'iday',
         'country_txt',
         'region_txt',
         'provstate',
         'city',
         'latitude',
         'longitude',
         'location',
         'summary',
         'attacktype1_txt',
         'targtype1_txt',
         'targsubtype1_txt',
         'gname',
         'nperps',
         'nkill',
         'nwound']
    dtype = {
        "iyear": int,
        "imonth": int,
        "iday": int,
        "country_txt": str,
        "region_txt": str,
        "provstate": str,
        "city": str,
        "latitude": "float64",
        "longitude": "float64",
        "location" : str,
        "summary": str,
        "attacktype1_txt": str,
        "targtype1_txt": str,
        "targsubtype1_txt": str,
        "gname": str,
        "nperps": int,
        "nkill": int,
        "nwound": int
    }
    df = pd.read_csv(csv_path, encoding="iso-8859-1",usecols=cols, dtype=dtype)
    return df

def load_csv_type_2(csv_path) -> pd.DataFrame:
    dtype = {
        "Date": str,
        "City": str,
        "Country": str,
        "Perpetrator": str,
        "Weapon": str,
        "Injuries": int,
        "Fatalities": int,
        "Description": str

    }
    df = pd.read_csv(csv_path, encoding="iso-8859-1",dtype=dtype)
    return df