import streamlit as st
import pandas as pd
import os
import sqlite3
import numpy as np

from source.extract import Extract
from source.transform import Transform
from source.load import Load

# Função principal

def main():
    st.set_page_config(page_title="ETL com Streamlit", layout="wide")

    load = Load()

    # =========================
    # 1) Conectar ao Banco
    # =========================
    st.sidebar.header("Configuração do Banco")
    db_file = st.sidebar.text_input("Nome/arquivo do banco", "uber.db")

    if "db_name" not in st.session_state:
        st.session_state.db_name = None
        st.session_state.conn = None

    if st.sidebar.button("Conectar"):
        if db_file:
            if not os.path.exists(db_file):
                load._verify_and_create_db(db_file)
            st.session_state.db_name = db_file
            st.session_state.conn = sqlite3.connect(db_file, check_same_thread=False)
            st.sidebar.success(f"Conectado ao banco: {db_file}")
        else:
            st.sidebar.error("Informe um arquivo .db válido")

    if st.session_state.conn:
        # NOVA ORDEM DE ABAS
        tab_select, tab_create, tab_insert, tab_etl = st.tabs([
            "SELECT", 
            "CREATE TABLE", 
            "INSERT", 
            "ETL"
        ])

        # =========================
        # ABA 1: SELECT
        # =========================
        with tab_select:
            st.header("SELECT")

            def get_tables():
                try:
                    query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
                    tables = pd.read_sql_query(query_tables, st.session_state.conn)
                    return tables['name'].tolist()
                except Exception as e:
                    st.error(f"Erro ao buscar tabelas: {e}")
                    return []

            if st.button("🔄 Atualizar Tabelas"):
                st.session_state["refresh_tables"] = True

            table_list = get_tables()

            if not table_list:
                st.warning("Nenhuma tabela encontrada no banco.")
            else:
                table_name = st.selectbox("Escolha a tabela", table_list)

                def get_columns(table):
                    try:
                        query_cols = f"PRAGMA table_info('{table}');"
                        cols_info = pd.read_sql_query(query_cols, st.session_state.conn)
                        return cols_info['name'].tolist()
                    except Exception as e:
                        st.error(f"Erro ao buscar colunas: {e}")
                        return []

                col_list = get_columns(table_name)

                selected_cols = st.multiselect(
                    "Escolha as colunas (deixe vazio para todas)",
                    options=col_list
                )

                where_condition = st.text_input("WHERE", placeholder='Ex: column >= 3')
                st.warning('Não insira o termo WHERE na caixa de texto acima, apenas as condições a serem verificadas.')

                if st.button("Executar SELECT"):
                    try:
                        cols = ", ".join(selected_cols) if selected_cols else "*"
                        df = load.select(st.session_state.db_name, table_name, cols, where_condition)
                        st.success(f"{len(df)} linhas carregadas")
                        st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error(f"Erro: {e}")

        # =========================
        # ABA 2: CREATE TABLE
        # =========================
        with tab_create:
            st.header("CREATE TABLE")

            new_table = st.text_input("Nome da nova tabela")

            if "new_columns" not in st.session_state:
                st.session_state.new_columns = []

            st.subheader("Definir Colunas da Tabela")

            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                col_name = st.text_input("Nome da coluna", key="col_name_input")
            with col2:
                col_type = st.selectbox("Tipo da coluna", ["TEXT", "INTEGER", "REAL"], key="col_type_input")
            with col3:
                if st.button("Adicionar coluna"):
                    if col_name:
                        st.session_state.new_columns.append((col_name, col_type))
                        st.success(f"Coluna '{col_name}' adicionada")
                    else:
                        st.error("Digite um nome de coluna")

            if st.session_state.new_columns:
                st.write("📋 Colunas definidas até agora:")
                for i, (cname, ctype) in enumerate(st.session_state.new_columns, start=1):
                    st.write(f"{i}. {cname} ({ctype})")

            if new_table and st.session_state.new_columns:
                if st.button("Criar tabela no banco"):
                    try:
                        dtype_map = {
                            "TEXT": str,
                            "INTEGER": np.int64,
                            "REAL": np.float64
                        }
                        column_data = {
                            col_name: pd.Series(dtype=dtype_map[col_type])
                            for col_name, col_type in st.session_state.new_columns
                        }
                        df_empty = pd.DataFrame(column_data)
                        load.create_table(df_empty, st.session_state.db_name, new_table)
                        st.success(f"Tabela '{new_table}' criada com colunas definidas!")
                    except Exception as e:
                        st.error(f"Erro ao criar tabela: {e}")

            st.markdown("---")
            st.subheader("Inserir dados via CSV")
            uploaded_file = st.file_uploader("Carregar CSV", type=["csv"], key="insert_csv")

            if uploaded_file and new_table:
                df_new = pd.read_csv(uploaded_file)
                st.dataframe(df_new.head())
                if st.button("Inserir dados no banco"):
                    try:
                        load.insert_data(df_new, st.session_state.db_name, new_table)
                        st.success(f"Dados inseridos em '{new_table}' com sucesso!")
                    except Exception as e:
                        st.error(f"Erro: {e}")

        # =========================
        # ABA 3: INSERT
        # =========================
        with tab_insert:
            st.header("INSERT")

            try:
                query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
                tables = pd.read_sql_query(query_tables, st.session_state.conn)
                table_list = tables['name'].tolist()
            except Exception as e:
                table_list = []
                st.error(f"Erro ao buscar tabelas: {e}")

            if table_list:
                table_name = st.selectbox("Escolha a tabela", table_list, key="insert_table")

                if table_name:
                    try:
                        query_cols = f"PRAGMA table_info('{table_name}');"
                        cols_info = pd.read_sql_query(query_cols, st.session_state.conn)
                        col_list = cols_info['name'].tolist()
                        col_types = cols_info['type'].tolist()
                    except Exception as e:
                        col_list, col_types = [], []
                        st.error(f"Erro ao buscar colunas: {e}")

                    if col_list:
                        st.subheader("Preencher valores da nova ocorrência:")

                        user_inputs = {}

                        for cname, ctype in zip(col_list, col_types):
                            if cname.lower() == "idx":
                                query_max = f'SELECT MAX("idx") as max_idx FROM "{table_name}";'
                                max_id_df = pd.read_sql_query(query_max, st.session_state.conn)
                                next_idx = int(max_id_df["max_idx"].iloc[0] or 0) + 1
                                st.info(f"'idx' será atribuído automaticamente: {next_idx}")
                                user_inputs[cname] = next_idx
                            else:
                                if ctype.upper() in ["INTEGER", "REAL"]:
                                    user_inputs[cname] = st.number_input(f"{cname} ({ctype})", key=f"input_{cname}")
                                else:
                                    user_inputs[cname] = st.text_input(f"{cname} ({ctype})", key=f"input_{cname}")

                        if st.button("Inserir ocorrência"):
                            try:
                                df_insert = pd.DataFrame([user_inputs])
                                load.insert_data(df_insert, st.session_state.db_name, table_name)
                                st.success(f"Ocorrência inserida em '{table_name}' com sucesso!")
                                st.write(df_insert)
                            except Exception as e:
                                st.error(f"Erro: {e}")
            else:
                st.warning("Nenhuma tabela encontrada no banco.")

        # =========================
        # ABA 4: ETL
        # =========================
        with tab_etl:
            st.header("ETL")

            file_path = st.text_input("Caminho para CSV (default: files/ncr_ride_bookings.csv)", 
                                      "files/ncr_ride_bookings.csv")
            if st.button("Rodar ETL"):
                try:
                    extract = Extract()
                    dataframe = extract.run(file_path=file_path)

                    transform = Transform(
                        frame=dataframe,
                        reason_columns=['Reason for cancelling by Customer', 'Incomplete Rides Reason', 'Driver Cancellation Reason'],
                        payment_columns=['Payment Method'],
                        status_columns=['Booking Status'],
                        car_model_columns=['Vehicle Type'],
                        incomplete_columns=['Incomplete Rides Reason'],
                    )
                    tables = transform.init_transform()

                    for name, frame in tables:
                        load.create_table(frame, st.session_state.db_name, name)
                        load.insert_data(frame, st.session_state.db_name, name)

                    st.success("ETL finalizado! Tabelas criadas e carregadas no banco.")
                    st.write("Tabelas carregadas:", [name for name, _ in tables])

                except Exception as e:
                    st.error(f"Erro no ETL: {e}")

if __name__ == "__main__":
    main()

