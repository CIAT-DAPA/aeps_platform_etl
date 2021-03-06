import conf as c
import pandas as pd
import translate as tr
import os
from os import listdir
import re
import numpy as np
import datetime
import shutil


## Method to process file. It checks the raw data and split in many files according to tables that should be saved.
## It splits process in two folders: New folder is for new records and Updates folder is for records which exist into database
## (string) file: Path of file which will be processed
## (object) cnn: Database connection
## (string) table_name: Name of table into database
def process_form(file, cnn, table_name):
    
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
    import_data = tr.apply_transformations_form(transformations, table_name_real, import_data)

    # Validations
    print("\t\t\tValidating data")
    import_data = tr.get_validations(validations, table_name_real, import_data, False)

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
    tr.save_form(records_new, keys[table_name].values, c.path_ouputs_new + table_name_real + ".csv")
    tr.save_form(records_update, keys[table_name].values, c.path_ouputs_updates + table_name_real + ".csv")
    return { 'new' : records_new.shape[0], 'updates': records_update.shape[0]  }

##
def process_survey(file, cnn):
    print("\t\t\tStarting")
    # Getting blocks of questions
    blocks = survey[["block","repeat"]].drop_duplicates()
    # Getting data 
    sheet_main = "aeps_production_event-plot"
    #plot = pd.read_excel(file, sheet_name = sheet_main)
    constant_name = True
    answers = pd.DataFrame(columns=["event","raw_value","question", "type","fixed_value","raw_units","fixed_units","validated"])
    for b in blocks.itertuples(index=True, name='Pandas') :
        print("\t\t\tBlock: " + getattr(b, "block") + " repeat: " + str(getattr(b, "repeat")))        
        questions = survey[survey.block == getattr(b, "block")][["id","question","type"]].drop_duplicates()
        questions["full_name"] = questions.question
        if(constant_name):
            q_rows = ((questions.question != "KEY") & (questions.question != "PARENT_KEY"))
            questions.loc[q_rows,"full_name"] = sheet_main + "-" + getattr(b, "block") + "-" + questions.loc[q_rows, "question"]
        
        # THis section is to know if we should search in the main sheet or others sheets
        if(getattr(b, "repeat") == 0):
            sheet_name = sheet_main
            data_raw = pd.read_excel(file, sheet_name = sheet_name)
            key_field = "KEY"
        elif(getattr(b, "repeat") == 1):
            sheet_name = sheet_main + "-" + getattr(b, "block")[:4]
            data_raw = pd.read_excel(file, sheet_name = sheet_name)
            key_field = "PARENT_KEY"

        data_raw = data_raw[questions["full_name"].values]
        data_raw = tr.trim_all_columns(data_raw)
        
        for q in questions.itertuples(index=True, name='Pandas') :
            if getattr(q, "id") != 0:
                print("\t\t\t\t" + getattr(q, "question"))
                data_a = data_raw[[key_field,getattr(q, "full_name")]]
                data_a.columns = ["event","raw_value"]
                data_a["question"] = getattr(q, "id")
                data_a["type"] = getattr(q, "type")                
                data_a["fixed_value"] = data_a["raw_value"]
                data_a["raw_units"] = ""
                data_a["fixed_units"] = ""
                # Transforming
                data_a = tr.apply_transformations_survey(transformations, data_a, getattr(q, "question"), data_raw)
                # Searching
                if(getattr(q, "type")=="unique" or getattr(q, "type")=="multiple"):
                    table_options = pd.read_sql_table("frm_options", cnn)
                    table_options = table_options.loc[table_options.question == getattr(q, "id"),["id","name"]]
                    data_a = pd.merge(data_a, table_options, left_on = "raw_value",right_on="name",how='inner') #data_a.set_index("raw_value").join(table_options.set_index("name")).reset_index()
                    data_a["fixed_value"] = data_a["id"]
                    data_a.drop('id', axis=1, inplace=True)
                    data_a.drop('name', axis=1, inplace=True)

                # Validating
                data_a = tr.get_validations(validations, "survey", data_a, True)                
                data_a["validated"] = 0
                data_a.loc[data_a["ERROR"] == "","validated"] = 1
                
                data_a.drop('ERROR', axis=1, inplace=True)
                data_a.columns = ["event","raw_value","question", "type","fixed_value","raw_units","fixed_units","validated"]                                
                answers = answers.append(data_a)
        
    tr.save_survey(answers[((answers.type == "int") | (answers.type == "double"))], c.path_ouputs_new + "far_responses_numeric.csv", "numeric") 
    tr.save_survey(answers[((answers.type == "date") | (answers.type == "time") | (answers.type == "datetime"))], c.path_ouputs_new + "far_responses_date.csv", "date")    
    tr.save_survey(answers[answers.type == "bool"], c.path_ouputs_new + "far_responses_bool.csv", "bool")
    tr.save_survey(answers[((answers.type == "unique") | (answers.type == "multiple"))], c.path_ouputs_new + "far_responses_options.csv", "options")
    tr.save_survey(answers[((answers.type != "int") & (answers.type != "double") & (answers.type != "date") & (answers.type != "time") & (answers.type != "datetime") & (answers.type != "bool") & (answers.type != "unique") & (answers.type != "multiple"))],c.path_ouputs_new + "far_responses_text.csv", "text")
    answers.drop('type', axis=1, inplace=True)
    answers.to_csv(c.path_ouputs_new + "far_answers.csv", index = False)

print("Clearing folders")
#for folder in [c.path_ouputs_new, c.path_ouputs_updates, c.path_logs]:
#    shutil.rmtree(folder)
#    os.mkdir(folder)

print("Translating process started")
# Loading files with raw data
path_data_files = listdir(c.path_inputs)
tables = c.tables_master
# Getting the form structure
form = pd.read_excel(c.path_form, sheet_name='form')
transformations = pd.read_excel(c.path_form, sheet_name='transformations')
validations = pd.read_excel(c.path_form, sheet_name='validations')
survey = pd.read_excel(c.path_form, sheet_name='survey')
#survey_sheet = "aeps_production_event-plot"
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
        result_form = process_form(c.path_inputs + f, db_connection,  t)
        print("\t\t\tNew records: " + str(result_form['new']) + " Updates: " + str(result_form['updates']))
    
    # Processing survey
    print("\t\tSurvey")
    result_survey = process_survey(c.path_inputs + f, db_connection)
    #print("\t\t\tNew records: " + str(result_survey['new']) + " Updates: " + str(result_survey['updates']))