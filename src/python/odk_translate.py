import conf as c
import pandas as pd
import os
from os import listdir
import re
import numpy as np

## Method which Trim whitespace from ends of each value across all series in dataframe
## (dataframe) df: Dataframe for cleaning
def trim_all_columns(df):
    trim_strings = lambda x: x.strip() if type(x) is str else x
    return df.applymap(trim_strings)

## Method which apply rules for transforming data into new dataset
## (dataframe) rules: Table, which has the rules to be applied to dataset
## (string) table_name: Name of table 
## (dataframe) data: Dataset which will get the transformations
def apply_transformations(rules, table_name, data):
    tmp_data = data

    # Getting rules
    transformations_tmp = rules[rules["table"] == table_name]
    
    ## Replace
    trs_replace = transformations_tmp[transformations_tmp["type"] == "replace"]
    if( trs_replace.shape[0] > 0 ):
        for tmp_field in trs_replace.field.unique():
            for row in trs_replace[trs_replace["field"] == tmp_field].itertuples(index=True, name='Pandas') :  
                tmp_data[tmp_field] = tmp_data[tmp_field].str.replace('^' + getattr(row, "value") + '$',getattr(row, "transform"))    
    ## Split
    trs_replace = transformations_tmp[transformations_tmp["type"] == "split"]
    if( trs_replace.shape[0] > 0 ):
        for tmp_field in trs_replace.field.unique():
            for row in trs_replace[trs_replace["field"] == tmp_field].itertuples(index=True, name='Pandas') :
                tmp_data[[getattr(row, "field"),getattr(row, "transform")]] = tmp_data[getattr(row, "field")].str.split(getattr(row, "value"),expand=True)

    return data

## Method which validates that fields are in good shape
## (dataframe) rules: Table, which has the rules to validate
## (string) table_name: Name of table 
## (dataframe) data: Dataset which will be validated
## (int) p: Number of process
def get_validations(rules, table_name, data, p):
    # Preparing data
    log = data
    log["ERROR"] = ""
    
    # Checking mandatory fields
    mandatory_tmp = rules[(rules["type"] == "mandatory") & (rules["table"] == table_name)]   
    
    for mdt in mandatory_tmp.itertuples(index=True, name='Pandas') :
        # Without condition                     # According to current process
        if((getattr(mdt, "condition") == "" or pd.isnull(getattr(mdt, "condition"))) or (getattr(mdt, "condition") == "process" and p == int(getattr(mdt, "condition_value")))):
            missing_values =  log[getattr(mdt, "field")].isna()
            if (missing_values[missing_values].shape[0] == 0):
                missing_values =  log[getattr(mdt, "field")].astype(str) == ""
            log.loc[missing_values, "ERROR"] = log.loc[missing_values, "ERROR"] + " Empty field: " + table_name + " : " + getattr(mdt, "field") + ","
    
    # Regular expressions
    reg_exp = rules[(rules["type"] == "reg_exp") & (rules["table"] == table_name)]
    for row in reg_exp.itertuples(index=True, name='Pandas') :
        # Without condition
        if(getattr(row, "condition") == "" or pd.isnull(getattr(row, "condition"))):
            missing_values =  log[getattr(row, "field")].astype(str).str.contains(getattr(row, "expression"), case = True, na=False, regex=True)
            log.loc[missing_values == False, "ERROR"] = log.loc[missing_values == False, "ERROR"] + "Invalid field (regular expression): " + table_name + " : " + getattr(row, "field") + ", "

    # Saving log of issues
    log_error = log["ERROR"] != ""
    if(log[log_error].shape[0] > 0):
        log[log_error].to_csv(c.path_logs + "validations-" + table_name + ".csv", index = False)    
    # Getting the data without issues
    log = log[log_error == False]
    log.drop('ERROR', axis=1, inplace=True)
    return log


def save(df, keys, path):
    if(df.shape[0] > 0):
        if (os.path.isfile(path)) :
            old =  pd.read_csv(path, dtype=str)
            df = pd.concat([df,old], sort = False)

        # Removing duplicates
        df = df.drop_duplicates(subset = keys, keep = 'last')
        df.to_csv(path, index = False)


## Method to process file. It checks the raw data and split in many files according to tables that should be saved.
## It splits process in two folders: New folder is for new records and Updates folder is for records which exist into database
## (int) process: Number the process to check
## (string) file: Path of file which will be processed
## (object) cnn: Database connection
## (string) table_name: Name of table into database
def process_file(process, file, cnn, table_name):
    
    # Getting the fields from 
    form_fields = ["form_field","form_key",table_name]
    f = form[form["process"] == process]
    form_tmp = f[form_fields]
    form_tmp = form_tmp[form_tmp[table_name].notna()] 
    
    # It does not have configuration
    if (form_tmp.shape[0] == 0):
        return { 'new' : 0, 'updates': 0  }
    # Getting the keys from form    
    keys = form_tmp[form_tmp.form_key == 1]

    # Loading data raw
    form_sheet = setup[setup.table == table_name].iloc[0][1]
    data_raw = pd.read_excel(file, sheet_name = form_sheet)
    
    # Cleaning data with empty spaces
    print("\t\t\tCleaning data")
    data_raw = trim_all_columns(data_raw)

    # Removing duplicates
    print("\t\t\tRemoving duplicates")
    import_data = data_raw[form_tmp.form_field.values]
    import_data = import_data.drop_duplicates(subset = keys.form_field.values, keep = 'last')

    # Comparing with data of the database
    print("\t\t\tComparing with data of the database")
    table = pd.read_sql_table(table_name, cnn)
    # Ordering datasets    
    import_data = import_data.sort_values(by=keys['form_field'].drop_duplicates().values.tolist())
    table = table.sort_values(by=keys[table_name].values.tolist())
    table = table[keys[table_name].values]

    # Fixing the columns names
    import_data.columns = form_tmp[table_name].values.tolist()
    table = table.reset_index(drop=True)
    import_data = import_data.reset_index(drop=True)

    # Transformations
    print("\t\t\tTransforming data")
    import_data = apply_transformations(transformations, table_name, import_data)

    # Validations
    print("\t\t\tValidating data")
    import_data = get_validations(validations, table_name, import_data, process)

    # Looking for records available into database    
    records_available = import_data.isin(table)
    records = records_available[records_available.columns.values[0]]
    records_new = import_data[records == False]
    records_update = import_data[records == True]    

    # Saving records
    print("\t\t\tSaving")
    save(records_new, keys[table_name].values, c.path_ouputs_new + table_name + ".csv")
    save(records_update, keys[table_name].values, c.path_ouputs_updates + table_name + ".csv")
    return { 'new' : records_new.shape[0], 'updates': records_update.shape[0]  }
    



print("Translating process started")
process_list = [1,2]
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = c.tables_master
# Getting the form structure
setup = pd.read_excel(c.path_form, sheet_name='setup') 
form = pd.read_excel(c.path_form, sheet_name='header')
transformations = pd.read_excel(c.path_form, sheet_name='transformations')
validations = pd.read_excel(c.path_form, sheet_name='validations')
# Getting database connection
print("Connecting database")
db_connection = c.connect_db()

for p in process_list:
    print("Process: " + str(p))
    for f in path_data_files:

        print("\tFiles: " + f)

        for t in tables:

            print("\t\tTable: " + t)        
            # Processing files
            result = process_file(p, c.path_inputs + f, db_connection,  t)
            print("\t\t\tNew records: " + str(result['new']) + " Updates: " + str(result['updates']))