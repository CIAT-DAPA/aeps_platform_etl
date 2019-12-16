import conf as c
import pandas as pd
from os import listdir
import datetime
import os
import re

## Method which save the file into the database
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
        new_data = pd.read_csv(c.path_ouputs_new + table_name + ".csv", dtype=str)

        # Getting dependencies from configuration
        dependencies_tmp = dependencies[dependencies["child_table"] == table_name]
        if(dependencies_tmp.shape[0] > 0):
            for d in dependencies_tmp.itertuples(index=True, name='Pandas'):
                # Getting all records of parent table
                parent = pd.read_sql_table(getattr(d, "parent_table"), cnn)                                
                field_child = getattr(d, "child_field")
                field_parent = getattr(d, "parent_field")
                # Getting just two fields from parent
                parent = parent[["id",field_parent]]                            
                parent[field_parent] = parent[field_parent].astype(str)
                parent["id"] = parent["id"].astype(str)
                new_data[field_child] = new_data[field_child].astype(str)
                # Mergin data
                #new_data.to_csv(c.path_logs + "debug-" + table_name + "-" + getattr(d, "parent_table") + "-before.csv", index = False)
                #parent.to_csv(c.path_logs + "debug-" + table_name + "-" + getattr(d, "parent_table") + "-before-parent.csv", index = False)
                new_data = new_data.set_index(field_child).join(parent.set_index(field_parent)).reset_index()                         
                #new_data.to_csv(c.path_logs + "debug-" + table_name + "-" + getattr(d, "parent_table") + "-mergin.csv", index = False)       
                if("index" in new_data.columns):
                    new_data.drop("index", axis=1, inplace=True)
                if(field_child in new_data.columns):
                    new_data.drop(field_child, axis=1, inplace=True)       
                # Replacing the name of column id by the child field name         
                new_data.columns = new_data.columns.str.replace('^id$',field_child)                
                #new_data.to_csv(c.path_logs + "debug-" + table_name + "-" + getattr(d, "parent_table") +"-after.csv", index = False)
                # Missing values
                missing = new_data[field_child].isna()
                new_data[field_child] = new_data[field_child].astype(str)
                # Saving log of issues
                log = new_data[missing]
                log["ERROR"] = table_name + ":" +field_parent + " Missing parent"
                if(log.shape[0] > 0):
                    log.to_csv(c.path_logs + "new-" + table_name + ".csv", index = False) 
                new_data = new_data[missing==False]
        
        # Getting addtional from configuration
        additional_tmp = additional[additional["table"] == table_name ]
        
        # Adding extra information
        if(additional_tmp.shape[0] > 0 and additional_tmp.iloc[0]["register_date"] == 1):
            now = datetime.datetime.now()        
            new_data['created'] = now
            new_data['updated'] = now        
        if(additional_tmp.shape[0] > 0 and additional_tmp.iloc[0]["has_enable"] == 1):
            new_data['enable'] = 1
            
        # Filling empty data
        columns_ommited = table.columns.isin(new_data.columns)
        columns_new = table.columns[columns_ommited == False]            
        for col in columns_new.values:
            if(col != "id"):
                new_data[col] = ""
        
        # Saving into database
        #new_data.to_csv(c.path_logs + "new-debug-" + table_name + ".csv", index = False) 
        new_data.to_sql(table_name, cnn, if_exists='append', chunksize=1000, index = False)
        

print("Adding process started")
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = ["con_countries","con_states","con_municipalities","soc_associations","soc_people","soc_technical_assistants","far_farms","far_plots", "far_production_events"]
surveys = ["far_responses_bool","far_responses_date","far_responses_numeric","far_responses_options","far_responses_text"]
# Getting the configurations
dependencies = pd.read_excel(c.path_parameters, sheet_name='dependencies')
additional = pd.read_excel(c.path_parameters, sheet_name='additional')
# Getting database connection
print("Connecting database")
db_connection = c.connect_db()

for t in tables:
    print("\tForm - Table: " + t)
    ## Processing files
    add(db_connection, t)

for s in surveys:
    print("\tSurvey - Table: " + s)
    ## Processing files
    add(db_connection, s)


        