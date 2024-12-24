import math
import pandas as pd
from tqdm import tqdm

from app.db.neo4j_database import driver



def insert_all_data(df: pd.DataFrame):
    query = """
        UNWIND $rows AS row
        MERGE (a:Attack_grope {name: row.gname})
        MERGE (t:Target {target_type: row.targtype1_txt})
        MERGE (c:Country {name: row.country_txt})
        CREATE (a) - [e1:EVENT {year: row.year}] -> (t)
        CREATE (a) - [e2:EVENT {year: row.year}] -> (c)
    """
    columns_to_check = ["gname", "targtype1_txt", "country_txt", "year"]
    filtered_df = df.dropna(subset=columns_to_check)
    rows = filtered_df.to_dict(orient="records")

    batch_size = 2000
    total_batches = math.ceil(len(rows) / batch_size)

    with driver.session() as session:
        for i in tqdm(range(total_batches), desc="Inserting data to neo4j", unit="batch"):
            batch = rows[i * batch_size: (i + 1) * batch_size]
            session.run(query, parameters={"rows": batch})
