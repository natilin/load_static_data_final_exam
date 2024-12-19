import numpy as np
import pandas as pd
from toolz import pipe
from app.repository.csv_repository import load_csv_type_1, load_csv_type_2
from app.utils import custom_to_datetime

def get_normalize_df_type1(df: pd.DataFrame) -> pd.DataFrame:
    """
    The function adds a date column with an appropriate format, replaces 'West Germany'
     and 'East' as 'Germany', updates negative and 'unknown values as null and removes duplicates.
    """

    df2 = df.copy()
    df2.rename(columns={"iyear": "year",
                        "imonth": "month",
                        "iday": "day",
                        }, inplace=True)
    df2.loc[df2["nperps"] <= 0, "nperps"] = np.nan
    df2.replace("Unknown", np.nan, inplace=True)
    df2["date"] = pd.to_datetime(df2[['year', 'month', 'day']], errors='coerce')
    df2["country_txt"] = df2["country_txt"].replace({"East Germany (GDR)": "Germany", "West Germany (FRG)": "Germany"})
    print(f"len of df2 before: {len(df2)}")
    df_cleaned = df2.drop_duplicates()
    print(f"len of df2 after: {len(df_cleaned)}")
    return df_cleaned



def get_normalize_df_type2(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2["Date"] = custom_to_datetime(df2["Date"],'%d-%b-%y', 2009)
    df2.rename(columns={"Date": "date",
                       "City": "city",
                       "Country": "country_txt",
                        "Perpetrator": "gname2",
                       "Injuries": "nwound",
                        "Weapon": "attacktype2_txt",
                        "Fatalities": "nkill",
                        "Description": "summary2"}, inplace=True)
    df_cleaned = df2.drop_duplicates()
    return df_cleaned



def add_unique_column(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2["unique_key"] = df2[["date", "country_txt", "city", "nkill", "nwound"]].astype(str).agg('_'.join, axis=1)
    return df2


def get_merged_csv():
    csv1_path = "./db/data/globalterrorismdb_0718dist.csv"
    csv2_path = "./db/data/RAND_Database_of_Worldwide_Terrorism_Incidents.csv"

    df1 = pipe(load_csv_type_1(csv1_path),
        get_normalize_df_type1,
               add_unique_column
               )

    df2 = pipe(load_csv_type_2(csv2_path),
               get_normalize_df_type2,
               add_unique_column
    )
    # print(f"len of df2: {len(df2)}")
    # df2_filtered = df2[~df2['unique_key'].isin(df1['unique_key'])]
    # print(f"len of df2 after filter: {len(df2_filtered)}")



    df2 = df2.drop(columns=['unique_key'])
    df1 = df1.drop(columns=['unique_key'])
    print("len 2 df before merge", len(df1) + len(df2))
    return pd.merge(df1, df2, how="outer",on=["date", "city", "country_txt", "nkill", "nwound"])



