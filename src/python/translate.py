import conf as c
import pandas as pd
from os import listdir

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
    # Getting the keys from form    
    keys = form_tmp[form_tmp.form_key == 1]

    # Loading data raw
    data_raw = pd.read_excel(file, sheet_name='Hoja 1')

    # Removing duplicates
    import_data = data_raw[form_tmp.form_field.values].drop_duplicates()

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

    # Looking for records available into database
    records = import_data.isin(table).values
    records_new = import_data[records == False]
    records_update = import_data[records == True]    

    # Saving records
    if(records_new.shape[0] > 0):
        records_new.to_csv(c.path_ouputs_new + table_name + ".csv", index = False)
    if(records_update.shape[0] > 0):
        records_update.to_csv(c.path_ouputs_updates + table_name + ".csv", index = False)
    print("\tNew records: " + str(records_new.shape[0]) + " Updates: " + str(records_update.shape[0]))



print("Starting process")
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = ["soc_associations","con_countries","con_states","con_municipalities"]
# Getting the form data
form = pd.read_excel(c.path_form, sheet_name='header')
#
print("Connecting database")
db_connection = c.connect_db()

for f in path_data_files:

    print("Processing: " + f)

    for t in tables:

        print("\tTable: " + t)        
        # Processing files
        process_file(c.path_inputs + f, db_connection,  t)