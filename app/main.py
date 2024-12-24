from app.db.elastic_search_database import init_elastic
from app.db.mongo_database import reset_mongo_db
from app.service.csv_service import get_merged_csv
from app.service.elastic_search_service import upload_df_to_elastic
from app.service.mongo_service import upload_df_to_mongo
from app.service.neo4j_service import upload_df_to_neo4j

if __name__ == '__main__':
    data = get_merged_csv()
    init_elastic()
    reset_mongo_db()
    upload_df_to_mongo(data)
    upload_df_to_neo4j(data)
    upload_df_to_elastic(data)







