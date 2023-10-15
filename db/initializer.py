from db_helpers import read_config_ini
import os
import pandas as pd

class DBInitializer:

    def __init__(self, db_client=None) -> None:
        """
        Creating database client.
        """
        assert db_client is not None
        self.db_client = db_client

    def initialize_db(self):
        """
        Method to initialize stock prices DB.
            - Loop through each csv file.
            - Clean the data 
            - Check if ticker data has a table in db, if so delete it.
            - Create ticker data in DB.
        """
        # Get csvs folder name.
        settings = read_config_ini()
        csvs_folder_path = settings['Folders.csvs_folder_path']

        failed_tickers = []
        # Loop through each csv file.
        for csv_file_name in os.listdir(csvs_folder_path):
            try:
                ticker = csv_file_name.split('_')[0]
                print(f"Ingesting {ticker.upper()}")
                csv_file_path = os.path.join(csvs_folder_path, csv_file_name)
                csv_df = self._preprocess_csv(csv_file_path)
                print(csv_df)
                print(ticker)
                self._check_if_table_exists(ticker)
                self.create_table_and_ingest(csv_df, ticker)
                print(f"Done ingesting {ticker}")
            except Exception as e:
                print(f"{ticker} Failed for {e}")
                failed_tickers.append(ticker)

        # Report failed tickers
        print(f"All tickers {len(os.listdir(csvs_folder_path))} || Failed {len(failed_tickers)} || Succeeded {len(os.listdir(csvs_folder_path)) - len(failed_tickers)}")

       
    def _from_df_to_pg(self, df, ticker):
        self._check_if_table_exists(ticker)

        conn = self.db_client.connection        
        df.to_sql(ticker, conn, if_exists='replace', index=False)
        self.db_client.commit()
        print(f"Data saved to PostgreSQL table '{ticker}' successfully.")
        
    def _preprocess_price_value(self, df_col):
        if df_col.dtype == 'float64':
            pass
        elif  df_col.dtype == 'int64':
            df_col = df_col.apply(lambda x: float(x))
        elif df_col.dtype == 'object':
            df_col = df_col.apply(lambda x: x.replace(',' , ''))
            if not df_col.empty and '..' in df_col[0]:
                df_col = df_col.apply(lambda x: float(x[2::]))
        return df_col
    
    def _preprocess_volume_value(self, df_col):
        if df_col.dtype == 'int64':
            pass
        elif  df_col.dtype == 'float64':
            df_col = df_col.apply(lambda x: int(x))
        elif df_col.dtype == 'object':
            df_col = df_col.apply(lambda x: x.replace(',' , ''))
            if not df_col.empty and '..' in df_col[0]:
                df_col = df_col.apply(lambda x: float(x[2::]))
        return df_col


    def _preprocess_csv(self, csv_file_path):
        """Preprocess csv file and returns pandas dataframe"""
        # Read csv file as dataframe.
        df = pd.read_csv(csv_file_path)
        
        # Change the datframe values into the desired shape/type. 
        df['High']   = self._preprocess_price_value(df['High'])
        df['Open']   = self._preprocess_price_value(df['Open'])
        df['Close']  = self._preprocess_price_value(df['Close'])
        df['Volume'] = self._preprocess_volume_value(df['Volume'])
        df['Low']    = self._preprocess_price_value(df['Low'])

        return df  

    def _check_if_table_exists(self, table_name):
        cur = self.db_client.cursor
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        table_exists = cur.fetchone()[0]

        if table_exists:
            # Table exists, so delete it
            cur.execute(f"DROP TABLE {table_name}")
            print(f"Table '{table_name}' deleted successfully.")
        else:
            print(f"Table '{table_name}' does not exist.")

        self.db_client.commit()


    def create_table_and_ingest(self, df, table_name):
        try:
            cur = self.db_client.cursor
            # Create a table with columns based on DataFrame columns and data types
            create_table_query = f"CREATE TABLE {table_name} ("
            for column_name, dtype in zip(df.columns, df.dtypes):
                if str(dtype) == 'int64':
                    column_type = 'INTEGER'
                elif str(dtype) == 'float64':
                    column_type = 'REAL'
                else:
                    column_type = 'TEXT'
                create_table_query += f"{column_name} {column_type}, "
            create_table_query = create_table_query.rstrip(', ') + ");"

            cur.execute(create_table_query)

            # Ingest the DataFrame into the PostgreSQL table
            for row in df.itertuples(index=False):
                insert_query = f"INSERT INTO {table_name} VALUES {tuple(row)};"
                cur.execute(insert_query)

            self.db_client.commit()
            print(f"Table '{table_name}' created and data ingested successfully.")

        except Exception as error:
            print(f"Error: {error}")




        
        
