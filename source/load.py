import pandas as pd
from sqlalchemy import create_engine
import sqlite3

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
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TEXT"  
        else:
            return "TEXT"

    def create_table_from_dataframe(self, df, db_name, table_name):

        if 'index' in df.columns:   # Index é uma palavra reservada do SQL, renomeando coluna para evitar erros.
            df = df.rename(columns={'index': 'idx'})

        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        columns = ', '.join([
            f'"{col}" {self._map_dtype_to_sql(df[col].dtype)}'
            for col in df.columns
        ])  
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'
        cursor.execute(create_table_sql)    # Executando Create Table
        
        for _, row in df.iterrows():
            values = tuple(row)
            col_names = ', '.join([f'"{col}"' for col in df.columns])
            placeholders = ', '.join(['?' for _ in df.columns])
            insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
            cursor.execute(insert_sql, values)  # Executando Insert
        
        conn.commit() # commitar mudanças
        conn.close() # fechar conexão

        print(columns)
        print(col_names)

        print(f"Tabela '{table_name}' criada e dados inseridos com sucesso!")