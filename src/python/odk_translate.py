import conf as c
import pandas as pd
import os
from os import listdir

## Method whichTrim whitespace from ends of each value across all series in dataframe
## (dataframe) df: Dataframe for cleaning
def trim_all_columns(df):
    trim_strings = lambda x: x.strip() if type(x) is str else x
    return df.applymap(trim_strings)

def save(df, keys, path):
    if(df.shape[0] > 0):
        if (os.path.isfile(path)) :
            old =  pd.read_csv(path, dtype=str)
            df = pd.concat([df,old])

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
    form_fields = ["form_sheet","form_field","form_key","mandatory",table_name]
    f = form[form["process"] == process]
    form_tmp = f[form_fields]
    form_tmp = form_tmp[form_tmp[table_name].notna()] 
    
    # It does not have configuration
    if (form_tmp.shape[0] == 0):
        return { 'new' : 0, 'updates': 0  }
    # Getting the keys from form    
    keys = form_tmp[form_tmp.form_key == 1]

    # Loading data raw
    data_raw = pd.read_excel(file, sheet_name='Hoja 1')
    
    # Cleaning data with empty spaces
    data_raw = trim_all_columns(data_raw)

    # Checking mandatory fields    
    mandatory_tmp = form_tmp[form_tmp.mandatory == 1]
    log = data_raw
    log["ERROR"] = ""
    for mdt in mandatory_tmp.itertuples(index=True, name='Pandas') :
        missing_values =  data_raw[getattr(mdt, "form_field")].isna()
        if (missing_values[missing_values].shape[0] == 0):
            missing_values =  data_raw[getattr(mdt, "form_field")].astype(str) == ""
        log.loc[missing_values, "ERROR"] = log.loc[missing_values, "ERROR"] + " Empty field: " + getattr(mdt, table_name) + " : " + getattr(mdt, "form_field") + ","
        
    log_error = log["ERROR"] != ""
    if(log[log_error].shape[0] > 0):
        log[log_error].to_csv(c.path_logs + "mandatory-" + table_name + ".csv", index = False)
    
    # Getting the data raw without issues
    data_raw = data_raw[log_error == False]

    # Filtering data duplicates
    import_data = data_raw[form_tmp.form_field.values]
        
    # Removing duplicates
    import_data = import_data.drop_duplicates(subset = keys.form_field.values, keep = 'last')

    # Getting values from database
    table = pd.read_sql_table(table_name, cnn)

    # Ordering datasets
    import_data = import_data.sort_values(by=keys['form_field'].values.tolist())
    table = table.sort_values(by=keys[table_name].values.tolist())
    table = table[keys[table_name].values]

    # Fixing the columns names
    import_data.columns = form_tmp[table_name].values.tolist()
    table = table.reset_index(drop=True)
    import_data = import_data.reset_index(drop=True)

    # Applying transformations
    transformations_tmp = transformations[transformations["table"] == table_name]
    
    ## Replace
    trs_replace = transformations_tmp[transformations_tmp["type"] == "replace"]
    if( trs_replace.shape[0] > 0 ):
        for tmp_field in trs_replace.field.unique():
            for row in trs_replace[trs_replace["field"] == tmp_field].itertuples(index=True, name='Pandas') :  
                import_data[tmp_field] = import_data[tmp_field].str.replace('^' + getattr(row, "value") + '$',getattr(row, "transform"))


    # Looking for records available into database
    records_available = import_data.isin(table)
    records = records_available[records_available.columns.values[0]]
    records_new = import_data[records == False]
    records_update = import_data[records == True]    

    # Saving records
    save(records_new, keys[table_name].values, c.path_ouputs_new + table_name + ".csv")
    save(records_update, keys[table_name].values, c.path_ouputs_updates + table_name + ".csv")
    return { 'new' : records_new.shape[0], 'updates': records_update.shape[0]  }
    



print("Translating process started")
process_list = [1,2]
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = c.tables_master
# Getting the form structure
form = pd.read_excel(c.path_form, sheet_name='header')
transformations = pd.read_excel(c.path_form, sheet_name='transformations')
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