from typing import List

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from app.utils import calculate_death_rate


from app.repository.mongoDB_repository import insert_all_data, create_index
from app.service.csv_service import get_merged_csv





def convert_terror_df_to_list(df: DataFrame) -> List:
    tqdm.pandas(desc="Converting data to list")
    return df.progress_apply(convert_row_of_terror_df_to_dict, axis=1).tolist()

def convert_row_of_terror_df_to_dict(row):
    return {
        "uuid": row["uuid"],
        "year": row["year"],
        "date": row["date"],
        "location":{
            "country": row["country_txt"],
            "city": row["city"],
            "region": row["region_txt"],
            "provstate": row["provstate"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "location_sub_details": row["location"],
        },
        "summary": row["summary"] if pd.notnull(row["summary"]) else row["summary2"],
        "attack_type": row["attacktype1_txt"] if pd.notnull(row["attacktype1_txt"])  else row["attacktype2_txt"],
        "target_type": row["targtype1_txt"],
        "target_subtype": row["targsubtype1_txt"],
        "group_name": row["gname"] if pd.notnull(row["gname"]) else row["gname2"],
        "terrorists_participating": row["nperps"],
        "killed": row["nkill"],
        "injured": row["nwound"],
        "deadly_rating": calculate_death_rate(row["nkill"], row["nwound"]),
    }


def upload_df_to_mongo(df):
    data_list = convert_terror_df_to_list(df)
    insert_all_data(data_list)
    print("Data inserted successfully to MongoDB")
    create_index()
    print("Index created successfully in MongoDB")

