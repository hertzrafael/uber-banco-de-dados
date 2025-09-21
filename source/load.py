import pandas as pd
from sqlalchemy import create_engine

class Load:

    def __init__(self):
        pass

    def upload_frame(self, conection, frame, column):
        df = frame

        df.to_sql(f"{column}", conection, if_exists='replace', index=False)

        print(f"adicionando tabela {column} ao banco de dados")