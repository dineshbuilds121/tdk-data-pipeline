import cx_Oracle
import numpy as np
import csv
from datetime import datetime
import time
import pandas as pd

PATH = '<YOUR DESIRED PATH>'
input_path = PATH + '/data/input'
output_path = PATH + '/data/output'

# Point the directory to Oracle Client files
time_stamp = datetime.now().strftime('%Y%m%d')
output_file_name = time_stamp + '_' + 'testOutput.txt'
cx_Oracle.init_oracle_client(lib_dir='<DIRP ATH TO >instantclient_basic/instantclient_21_3')

dsn = cx_Oracle.makedsn(
    host=<hostname>,
    port=<port_number>,
    sid=<sid>'
)
connection = cx_Oracle.connect(
    user=<USER_NAME>,
    password=<PASSWORD>,
    dsn=dsn
)

query = """ SELECT * FROM C_DUNS_V """

df_mda = pd.read_sql(query,con=connection)

df_mda.to_csv(output_path+'/'+output_file_name, sep="\t")
