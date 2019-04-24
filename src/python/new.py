import conf as c
import pandas as pd
from os import listdir
import datetime
import os
import re

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

        # Getting dependencies from configuration
        dependencies_tmp = dependencies[["table",table_name]]
        dependencies_tmp = dependencies_tmp[dependencies_tmp[table_name].notna()]

        if(dependencies_tmp.shape[0] > 0):        
            # Getting all records of parent table
            parent = pd.read_sql_table(dependencies_tmp["table"].iloc[0], cnn)
            # Getting only the fields for merging (id and ext_id)
            parent = parent[["id","ext_id"]]            
            # Mergin data
            parent.ext_id = parent.ext_id.astype(str)
            new_data[dependencies_tmp[table_name].iloc[0]] = new_data[dependencies_tmp[table_name].iloc[0]].astype(str)
            new_data = pd.merge(left = new_data, right = parent, how = 'inner', left_on = dependencies_tmp[table_name].iloc[0], right_on = 'ext_id')
            # Removing fields
            fields = new_data.columns.isin(["ext_id_y",dependencies_tmp[table_name].iloc[0]])
            fields = new_data.columns[fields == False]
            new_data = new_data[fields]
            # Replacing the columns names
            new_data.columns = new_data.columns.str.replace('^ext_id_x$','ext_id').str.replace('^id$',dependencies_tmp[table_name].iloc[0])
        
        # Getting addtional from configuration
        additional_tmp = additional[additional["table"] == table_name ]
        
        # Adding extra information
        if(additional_tmp.shape[0] > 0 and additional_tmp.iloc[0]["register_date"] == "yes"):
            now = datetime.datetime.now()        
            new_data['created'] = now
            new_data['updated'] = now        
        if(additional_tmp.shape[0] > 0 and additional_tmp.iloc[0]["has_enable"] == "yes"):
            new_data['enable'] = 1
            
        # Filling empty data
        columns_ommited = table.columns.isin(new_data.columns)
        columns_new = table.columns[columns_ommited == False]            
        for col in columns_new.values:
            if(col != "id"):
                new_data[col] = ""

        # Saving into database
        new_data.to_sql(table_name, cnn, if_exists='append', chunksize=1000, index = False)
        

print("Adding process started")
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = ["soc_associations","con_countries","con_states","con_municipalities"]
# Getting the configurations
dependencies = pd.read_excel(c.path_parameters, sheet_name='dependencies')
additional = pd.read_excel(c.path_parameters, sheet_name='additional')
# Getting database connection
print("Connecting database")
db_connection = c.connect_db()

for t in tables:
    print("\tTable: " + t)
    ## Processing files
    add(db_connection, t)

        