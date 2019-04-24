from sqlalchemy import create_engine
import pandas as pd
import pymysql.cursors

# Global 

## Variables
env = ".dev"
path_root = "G:\\CIAT\\Code\\BigData\\aeps_platform_etl\\src\\"
path_conf = "configurations\\"
path_form = path_root + path_conf + "form" + env + ".xlsx"
path_parameters = path_root + path_conf + "configuration" + env + ".xlsx"
path_inputs = path_root + "inputs\\"
path_ouputs = path_root + "outputs\\"
path_ouputs_new = path_ouputs + "new\\"
path_ouputs_updates = path_ouputs + "updates\\"

## Process
tables_master = ["con_countries","con_states","con_municipalities","soc_associations","soc_people"]

## Load Configurations
parameters = pd.read_excel(path_parameters, sheet_name='global')

## Method for getting values from configuration files
# (string) name: Name of parameter
def get_parameter(name):    
    value = parameters[parameters.parameter == name].iloc[0]['value'] 
    return str(value)

## Method which connects with the database
def connect_db():
    host = get_parameter('database_host')
    port = get_parameter('database_port')
    user = get_parameter('database_user')
    pwd = get_parameter('database_pwd')
    schema = get_parameter('database_schema')
    db_url = "mysql+mysqldb://" + user + ":" + pwd + "@" + host + ":" + port + "/" + schema
    return create_engine(db_url)