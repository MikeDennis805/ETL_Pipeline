import requests
import json
import pandas as pd
import psycopg2


#API endpoint
url = "https://v6.exchangerate-api.com/v6/492c04352240e1d42c7f26f6/latest/USD"
class Extract:
    @staticmethod
    def exchange_rate_data(url:str):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get fetch extract data:{response.status_code}")
        
class Transform:
    @staticmethod
    def transform_exchange_rate_data(raw_data:dict):
        base_currency = raw_data.get('base_code', 'USD')
        conversion_rates = raw_data.get('conversion_rates', {})
        df = pd.DataFrame(list(conversion_rates.items()), columns = ['currency_code', 'exchange_rate'])
        df['base_currency'] = base_currency
        df = df.sort_values(by = 'currency_code').reset_index(drop = True)

        return df
    
class Load:
    def __init__(self,db_name,user,password,host = 'localhost',port = 5432):
        #Establish a connection to Postgresql database.
        self.connection = psycopg2.connect(
            dbname = db_name,
            user = user,
            password = password,
            host = host,
            port = port
         )
        self.cursor = self.connection.cursor()
    

        # Create Table
    def create_table(self):
        creating_table_query = """
        CREATE TABLE IF NOT EXISTS exchange_rates(
        id SERIAL PRIMARY KEY,
        currency_code VARCHAR(50),
        exchange_rate NUMERIC
        base_currency VARCHAR(50)
        );
        """
        self.cursor.execute(creating_table_query)
        self.connection.commit()

    def insert_data(self, df):
        insert_query = """
        INSERT INTO exchange_rates (currency_code, exchange_rate, base_currency)
        VALUES (%s, %s, %s);
        """     
        for index, row in df.iterrows():
            self.cursor.execute(insert_query, (row['currency_code'], row['exchange_rate'], row['base_currency']))
        self.connection.commit()  

if __name__ == "__main__":
    # Step 1: Extract data
    raw_data = Extract.exchange_rate_data(url)

    # Step 2: Transform the data
    df = Transform.transform_exchange_rate_data(raw_data)

    # Step 3: Load the data
    loader = Load(
        db_name = "dennis_project",
        user = "dennis_user",
        password = "1234",
        host = "localhost",
        port = 5432
    )
    loader.create_table()
    loader.insert_data(df)
    
    



        


