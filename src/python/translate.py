import pandas as pd
import os
import conf as c
import datetime

## Method which Trim whitespace from ends of each value across all series in dataframe
## (dataframe) df: Dataframe for cleaning
def trim_all_columns(df):
    trim_strings = lambda x: x.strip() if type(x) is str else x
    return df.applymap(trim_strings)

## Method which apply rules for transforming data into new dataset
## (dataframe) rules: Table, which has the rules to be applied to dataset
## (string) table_name: Name of table 
## (dataframe) data: Dataset which will get the transformations
def apply_transformations_form(rules, table_name, data):
    tmp_data = data

    # Getting rules
    transformations_tmp = rules[rules["table"] == table_name]

    if( transformations_tmp.shape[0] > 0 ):
        for tmp_field in transformations_tmp.field.unique():
            for row in transformations_tmp[transformations_tmp["field"] == tmp_field].itertuples(index=True, name='Pandas') :  
                # Form
                if(getattr(row, "type") == "replace"):
                    tmp_data[tmp_field] = tmp_data[tmp_field].str.replace('^' + getattr(row, "value") + '$',getattr(row, "transform"))
                elif (getattr(row, "type") == "split"):
                    tmp_data[[getattr(row, "field"),getattr(row, "transform")]] = tmp_data[getattr(row, "field")].str.split(getattr(row, "value"),expand=True)
                elif (getattr(row, "type") == "add"):
                    tmp_data[getattr(row, "field")] = getattr(row, "transform")
    return data           

## Method which apply rules for transforming data into new dataset
## (dataframe) rules: Table, which has the rules to be applied to dataset
## (string) table_name: Name of table 
## (dataframe) data: Dataset which will get the transformations
def apply_transformations_survey(rules, data, field, full_data):
    tmp_data = data

    # Getting rules
    transformations_tmp = rules[rules["table"] == "survey"]

    if( transformations_tmp.shape[0] > 0 ):
        for tmp_field in transformations_tmp.field.unique():
            for row in transformations_tmp[transformations_tmp["field"] == tmp_field].itertuples(index=True, name='Pandas') :                 
                if(field == getattr(row, "field")):
                    if (getattr(row, "type") == "unit"):
                        field_units = full_data.columns.str.contains(getattr(row, "transform"))
                        col = str(full_data.columns[field_units == True].values[0])
                        tmp_data["raw_units"] = full_data[col]
                    elif (getattr(row, "type") == "multiply"):
                        if(getattr(row, "condition") == "unit"):
                            records = tmp_data.raw_units == getattr(row, "value")
                            tmp_data.loc[records,"fixed_value"] = tmp_data.loc[records,"raw_value"] * getattr(row, "transform")
                            tmp_data.loc[records,"fixed_units"] = getattr(row, "units")
    return data

## Method which validates that fields are in good shape
## (dataframe) rules: Table, which has the rules to validate
## (string) table_name: Name of table 
## (dataframe) data: Dataset which will be validated
## (bool) error: Set if you want to get errors
def get_validations(rules, table_name, data, error):
    # Preparing data
    log = data
    log["ERROR"] = ""
    
    # Checking mandatory fields
    mandatory_tmp = rules[(rules["type"] == "required") & (rules["table"] == table_name)]   
    
    for mdt in mandatory_tmp.itertuples(index=True, name='Pandas') :
        if(getattr(mdt, "field") in log.columns.values):
            # Without condition
            if(getattr(mdt, "condition") == "" or pd.isnull(getattr(mdt, "condition"))) :            
                missing_values =  log[getattr(mdt, "field")].isna()
                if (missing_values[missing_values].shape[0] == 0):
                    missing_values =  log[getattr(mdt, "field")].astype(str) == ""
                log.loc[missing_values, "ERROR"] = log.loc[missing_values, "ERROR"] + getattr(mdt, "message") + ", "
    
    # Regular expressions
    reg_exp = rules[(rules["type"] == "reg_exp") & (rules["table"] == table_name)]
    for row in reg_exp.itertuples(index=True, name='Pandas') :
        if(getattr(row, "field") in log.columns.values):
            # Without condition
            if(getattr(row, "condition") == "" or pd.isnull(getattr(row, "condition"))):
                missing_values =  log[getattr(row, "field")].astype(str).str.contains(getattr(row, "expression"), case = True, na=False, regex=True)
                log.loc[missing_values == False, "ERROR"] = log.loc[missing_values == False, "ERROR"] + getattr(row, "message") + ", "

    # Saving log of issues
    log_error = log["ERROR"] != ""
    if(log[log_error].shape[0] > 0):
        log[log_error].to_csv(c.path_logs + "validations-" + table_name + ".csv", index = False)    
    # Getting the data without issues
    log = log[log_error == False]
    if(error == False):
        log.drop('ERROR', axis=1, inplace=True)
    return log

## Method that transform numeric dates (excel) in normal dates
## (string) xldate: Date in format numeric
def xldate_to_datetime(xldate):
    
    if(xldate == ""):
        return ""

    temp = datetime.datetime(1900, 1, 1)
    delta = datetime.timedelta(days=xldate)
    final = temp+delta
    return final.strftime("%Y-%m-%d %H:%M:%S")


## Method that save forms into a file
## (dataframe) df: Data which will be saved
## (Serie) keys: All key fields 
## (string) path: File name
def save_form(df, keys, path):
    if(df.shape[0] > 0):
        if (os.path.isfile(path)) :
            old =  pd.read_csv(path, dtype=str)
            df = pd.concat([df,old], sort = False)

        # Removing duplicates
        df = df.astype(str)
        df = df.drop_duplicates(subset = keys, keep = 'last')
        #df.drop_duplicates(df.index[0], inplace = True)
        df.to_csv(path, index = False)

## Method that save surveys into a file
## (dataframe) df: Data which will be saved
## (string) path: File name
def save_survey(df, path):
    df.drop('type', axis=1, inplace=True)
    if(df.shape[0] > 0):
        df.to_csv(path, index = False)