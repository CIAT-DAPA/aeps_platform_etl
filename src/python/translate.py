import conf as c
import pandas as pd
from os import listdir

## Method to process file
## (string) file: Path of data raw
## (object) cnn: Database connection
## (dataframe) form: Form configuration
## (string) table_name: Name of table into database
def process_file(file, cnn, form, table_name):
    
    ### Getting the keys from form
    keys = form[form.form_key == 1]

    ### Loading data raw
    data_raw = pd.read_excel(file, sheet_name='Hoja 1')

    ### Removing duplicates
    import_data = data_raw[form.form_field.values].drop_duplicates()

    ### Getting values from database
    table = pd.read_sql_table(table_name, cnn)

    ### Ordering datasets
    import_data = import_data.sort_values(by=keys['form_field'].values.tolist())
    table = table.sort_values(by=keys['db_field'].values.tolist())
    table = table[keys['db_field'].values]

    ### Fixing the columns names
    import_data.columns = form['db_field'].values.tolist()
    table = table.reset_index(drop=True)
    import_data = import_data.reset_index(drop=True)

    ### Looking for records available into database
    #records = import_data[keys['form_field'].values].values == table[keys['db_field'].values].values
    #records = import_data.merge(table,on=form['db_field'].values.tolist())
    records = import_data.isin(table).values
    records_new = import_data[records == False]
    records_update = import_data[records == True]

    ### Fixing the columns names
    #records_new.columns = form['db_field'].values.tolist()
    #records_update.columns = form['db_field'].values.tolist()

    ### Saving records
    if(records_new.shape[0] > 0):
        records_new.to_csv(c.path_ouputs_new + table_name + ".csv", index = False)
    if(records_update.shape[0] > 0):
        records_update.to_csv(c.path_ouputs_updates + table_name + ".csv", index = False)
    print("\tNew records: " + str(records_new.shape[0]) + " Updates: " + str(records_update.shape[0]))


## Load raw data
path_data_files = listdir(c.path_inputs)
tables = ["soc_associations","con_countries","con_states","con_municipalities"]

print("Connecting database")
db_connection = c.connect_db()

for f in path_data_files:

    print("Processing: " + f)

    for t in tables:

        print("\tTable: " + t)
        ## Declaring variables        
        form = pd.read_excel(c.path_form, sheet_name='header')
        form = form[form.db_table == t]
        ## Processing files
        process_file(c.path_inputs + f, db_connection, form, t)

        