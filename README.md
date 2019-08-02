# tabl2sql
package for loading tabular data to SQL databases

logging module is used
---------example, to change logging level to INFO (most logs in this package):
	logging.basicConfig(level=logging.INFO)
    

example run:
import logging
logging.basicConfig(level=logging.INFO)
import tabl2sql
args = ['-dirs', '/home/share/data/', 
        '-table', 'table_test', 
        '-sql', 'oracle', 
        '-driver', 'cx_oracle', 
        '-user', 'user', 
        '-pw', 'user@server:1521', 
        '-target', 'ORCL1', 
        '-sep', '\\t']
tabl2sql.transform.main(args)
