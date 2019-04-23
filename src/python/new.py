import conf as c
import pandas as pd
from os import listdir
import datetime
import os

## Method to process file
## (string) file: Path of data raw
## (object) cnn: Database connection
## (dataframe) form: Form configuration
## (string) table_name: Name of table into database
def add(cnn, table_name):

    ### Loading data raw
    if(os.path.isfile(c.path_ouputs_new + table_name + ".csv")):
        
        # Getting data from database
        table = pd.read_sql_table(table_name, cnn)

        # Loading data from file
        new_data = pd.read_csv(c.path_ouputs_new + table_name + ".csv")

        ### adding extra information
        now = datetime.datetime.now()        
        new_data['created']  = now
        new_data['updated']  = now

        columns_ommited = table.columns.isin(new_data.columns)
        columns_new = table.columns[columns_ommited == False]
        for col in columns_new.values:
            if(col != "id"):
                new_data[col] = ""

        print(new_data)

        ### Removing duplicates    
        new_data.to_sql(table_name, cnn, if_exists='append', chunksize=1000, index = False)
        print(new_data)


#tables = ["soc_associations","con_countries","con_states","con_municipalities"]
tables = ["con_countries"]

print("Connecting database")
db_connection = c.connect_db()

for t in tables:

    print("\tTable: " + t)
    ## Processing files
    add(db_connection, t)

        