from app.repository.neo4j_repository import insert_all_data
from app.service.csv_service import get_merged_csv


def upload_df_to_neo4j():
    df = get_merged_csv()
    insert_all_data(df)
    print("Data inserted successfully to neo4j")