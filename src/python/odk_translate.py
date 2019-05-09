import conf as c
import pandas as pd
import translate as tr
import os
from os import listdir
import re
import numpy as np

## Method to process file. It checks the raw data and split in many files according to tables that should be saved.
## It splits process in two folders: New folder is for new records and Updates folder is for records which exist into database
## (string) file: Path of file which will be processed
## (object) cnn: Database connection
## (string) table_name: Name of table into database
def process_file(file, cnn, table_name):
    
    # Getting the fields from 
    form_fields = ["form_sheet","form_field","form_key",table_name]
    form_tmp = form[form_fields]
    form_tmp = form_tmp[form_tmp[table_name].notna()] 
    
    # It does not have configuration
    if (form_tmp.shape[0] == 0):
        return { 'new' : 0, 'updates': 0  }
    # Getting the keys from form    
    keys = form_tmp[form_tmp.form_key == 1]

    # Loading data raw
    sheets = form_tmp.form_sheet.drop_duplicates().values
    data_raw = pd.DataFrame()
    for s in sheets :
        if(data_raw.shape[0] == 0):
            data_raw = pd.read_excel(file, sheet_name = s)
        else:
            data_raw_tmp = pd.read_excel(file, sheet_name = s)
            # Mergin data of different sheets          
            data_raw_tmp = data_raw_tmp.set_index("PARENT_KEY")
            #data_raw_tmp.drop("PARENT_KEY", axis=1, inplace=True)
            data_raw = data_raw.set_index("KEY")
            #data_raw.drop("KEY", axis=1, inplace=True)
            data_raw = data_raw_tmp.join(data_raw).reset_index()    
    
    table_name_real = table_name
    # Fixing to real table name
    if("soc_people" in table_name):
        table_name_real = "soc_people"
    elif("con_countries" in table_name):
        table_name_real = "con_countries"
    elif("con_states" in table_name):
        table_name_real = "con_states"
    elif("con_municipalities" in table_name):
        table_name_real = "con_municipalities"
    
    # Cleaning data with empty spaces
    print("\t\t\tTrim data")
    data_raw = tr.trim_all_columns(data_raw)
    
    # Removing duplicates
    print("\t\t\tRemoving duplicates")
    import_data = data_raw[form_tmp.form_field.values]
    import_data = import_data.drop_duplicates(subset = keys.form_field.values, keep = 'last')
    # Fixing the columns names
    import_data.columns = form_tmp[table_name].values.tolist()

    # Transformations
    print("\t\t\tTransforming data")
    import_data = tr.apply_transformations(transformations, table_name_real, import_data)

    # Validations
    print("\t\t\tValidating data")
    import_data = tr.get_validations(validations, table_name_real, import_data)

    # Comparing with data of the database
    print("\t\t\tComparing with data of the database")    
    table = pd.read_sql_table(table_name_real, cnn)
    # Ordering datasets by keys fields
    import_data = import_data.sort_values(by=keys[table_name].drop_duplicates().values.tolist())
    table = table.sort_values(by=keys[table_name].drop_duplicates().values.tolist())
    table = table[keys[table_name].values]
    
    # Reseting the index
    table = table.reset_index(drop=True)
    import_data = import_data.reset_index(drop=True)    
    
    # Looking for records available into database    
    records_available = import_data.isin(table)
    records = records_available[records_available.columns.values[0]]
    records_new = import_data[records == False]
    records_update = import_data[records == True]    

    # Saving records
    print("\t\t\tSaving")
    tr.save(records_new, keys[table_name].values, c.path_ouputs_new + table_name_real + ".csv")
    tr.save(records_update, keys[table_name].values, c.path_ouputs_updates + table_name_real + ".csv")
    return { 'new' : records_new.shape[0], 'updates': records_update.shape[0]  }
    
print("Translating process started")
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = c.tables_master
# Getting the form structure
form = pd.read_excel(c.path_form, sheet_name='form')
transformations = pd.read_excel(c.path_form, sheet_name='transformations')
validations = pd.read_excel(c.path_form, sheet_name='validations')
# Getting database connection
print("Connecting database")
db_connection = c.connect_db()

# Getting inputs files
for f in path_data_files:
    print("\tFile: " + f)
    # Processing tables
    for t in tables:
        print("\t\tTable: " + t)        
        # Processing files
        result = process_file(c.path_inputs + f, db_connection,  t)
        print("\t\t\tNew records: " + str(result['new']) + " Updates: " + str(result['updates']))