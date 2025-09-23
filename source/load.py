import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import os

class Load:

    def __init__(self):
        pass

    def upload_frame(self, conection, frame, column):
        df = frame

        df.to_sql(f"{column}", conection, if_exists='replace', index=False)

        print(f"adicionando tabela {column} ao banco de dados")

    def _map_dtype_to_sql(self, dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "REAL"
        elif pd.api.types.is_bool_dtype(dtype):
            return "INTEGER"
        else:
            return "TEXT"
        
    def _verify_and_create_db(self, db_name):
        """Verifica se o banco de dados existe. Se não, cria o banco de dados."""
        if not os.path.exists(db_name):

            conn = sqlite3.connect(db_name)
            conn.close()  
            print(f"Banco de dados '{db_name}' criado com sucesso!")
        else:
            print(f"O banco de dados '{db_name}' já existe.")

    def create_table(self, df, db_name, table_name):

        if 'index' in df.columns:
            df = df.rename(columns={'index': 'idx'})

        self._verify_and_create_db(db_name)

        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            columns = ', '.join([
                f'"{col}" {self._map_dtype_to_sql(df[col].dtype)}'
                for col in df.columns
            ])  

            create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'
            cursor.execute(create_table_sql)

            print(f"Tabela '{table_name}' criada com sucesso!")
            print("Colunas:", columns)


    def insert_data(self, df, db_name, table_name):

        if 'index' in df.columns:
            df = df.rename(columns={'index': 'idx'})

        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            col_names = ', '.join([f'"{col}"' for col in df.columns])
            placeholders = ', '.join(['?' for _ in df.columns])
            insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'

            cursor.executemany(insert_sql, df.values.tolist())

            conn.commit()

            print(f"Dados inseridos na tabela '{table_name}' com sucesso!")
            print("Colunas:", col_names)

    def select(self, db_name, table_name, columns='*', where=None):
        
        if not os.path.exists(db_name):
            raise FileNotFoundError(f"O banco de dados '{db_name}' não foi encontrado!")
        
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        query = f'SELECT {columns} FROM "{table_name}"'
        if where:
            query += f' WHERE {where}'

        cursor.execute(query)
        rows = cursor.fetchall()
        
        col_names = [desc[0] for desc in cursor.description]

        conn.close()

        return pd.DataFrame(rows, columns=col_names)
            
          