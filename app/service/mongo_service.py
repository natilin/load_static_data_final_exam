from typing import List

from pandas import DataFrame


def convert_terror_df_to_list(df: DataFrame) -> List:
    pass

def convert_row_of_terror_df_to_dict(row):
    return {
        "year": row["year"],
        "date": row["date"],
        "location":{
            "country": row["country_txt"],
            "city": row["city"],
            "provstate": row["provstate"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "location_sub_details": row["location"],
        },
        "summary": row["summary"] if row["summary"].notnull() else row["summary2"],
        "attack_type": row["attacktype1_txt"] if row["attacktype1_txt"].notnull() else row["attacktype2_txt"],
        "target_type": row["targtype1_txt"],
        "target_subtype": row["targsubtype1_txt"],
        "group_name": row["gname"] if row["gname"].notnull() else row["gname2"],
        "terrorists_participating": row["nperps"],
        "killed": row["nkill"],
        "injured": row["nwound"],
    }
