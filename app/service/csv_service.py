import math
import uuid

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
    df_cleaned = df2.drop_duplicates()
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


def get_normalize_merged_df(merged_df):
    copy_merged_df = merged_df.copy()
    copy_merged_df["date"] = copy_merged_df["date"].astype(object).where(copy_merged_df["date"].notnull(), None)
    copy_merged_df["year"] = copy_merged_df.apply(
        lambda row: row["date"].year if pd.notna(row["date"]) else row["year"], axis=1
    )
    copy_merged_df.replace("Unknown", np.nan, inplace=True)
    copy_merged_df["uuid"] = pd.Series([str(uuid.uuid4()) for _ in range(len(copy_merged_df))], index=copy_merged_df.index)

    return copy_merged_df


def get_merged_csv():
    csv1_path = "./db/data/globalterrorismdb_0718dist.csv"
    csv2_path = "./db/data/RAND_Database_of_Worldwide_Terrorism_Incidents.csv"

    df1 = pipe(load_csv_type_1(csv1_path),
        get_normalize_df_type1,
               )

    df2 = pipe(load_csv_type_2(csv2_path),
               get_normalize_df_type2,
    )


    merged_df =  pd.merge(df1, df2, how="outer",on=["date", "city", "country_txt", "nkill", "nwound"])
    return get_normalize_merged_df(merged_df)