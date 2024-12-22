from app.db.mongo_database import reset_mongo_db
from app.service.mongo_service import upload_df_to_mongo
from app.service.neo4j_service import upload_df_to_neo4j

if __name__ == '__main__':
    reset_mongo_db()
    upload_df_to_mongo()
    upload_df_to_neo4j()







