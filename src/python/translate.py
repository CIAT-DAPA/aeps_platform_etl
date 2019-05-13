import pandas as pd
import os
import conf as c

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
    
    ## Add
    trs_add = transformations_tmp[transformations_tmp["type"] == "add"]
    if( trs_add.shape[0] > 0 ):
        for tmp_field in trs_add.field.unique():
            for row in trs_add[trs_add["field"] == tmp_field].itertuples(index=True, name='Pandas') :
                tmp_data[getattr(row, "field")] = getattr(row, "transform")

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
                log.loc[missing_values, "ERROR"] = log.loc[missing_values, "ERROR"] + getattr(row, "message") + ", "

    # Saving log of issues
    log_error = log["ERROR"] != ""
    if(log[log_error].shape[0] > 0):
        log[log_error].to_csv(c.path_logs + "validations-" + table_name + ".csv", index = False)    
    # Getting the data without issues
    log = log[log_error == False]
    if(error == False):
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