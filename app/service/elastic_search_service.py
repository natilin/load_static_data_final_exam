from typing import List

from pandas import DataFrame
from tqdm import tqdm

from app.repository.elastic_search_repository import insert_all_data_elastic
from app.service.csv_service import get_merged_csv


def convert_df_to_elastic_model_list(df: DataFrame) -> List:
    df['summary'] = df['summary'].fillna(df['summary2'])
    df = df.dropna(subset=['summary'])
    df = df.dropna(subset=['date'])
    tqdm.pandas(desc="converting dataframe to elastic model")
    return df.progress_apply(convert_row_of_terror_to_elastic_model, axis=1).to_list()



def convert_row_of_terror_to_elastic_model(row):
    return {
        "_id": row["uuid"],
        "_index": "terror_event",
        "_source": {
            "summary": row["summary"],
            "date": row["date"]
        }
    }

def upload_df_to_elastic(df: DataFrame):
    data_list = convert_df_to_elastic_model_list(df)
    insert_all_data_elastic(data_list)