# tabl2sql
package for loading tabular data to SQL databases
logging module is used

# todo
add ability to accept excel files
use hdf5 to allow any file size to work on lower RAM. perhaps dask after to speed up. alternatively could add steps for (1)check file size, (2)break up if large then (3) run main process one by one or in chunks
parallelize

# example run
import logging
logging.basicConfig(level=logging.INFO)
import tabl2sql
args = ['-dirs', '/home/share/data/', 
        '-table', 'new_table', 
        '-sql', 'mysql', 
        '-driver', 'mysqlconnector', 
        '-user', 'user', 
        '-pw', 'password', 
        '-target', 'new_db', 
        '-sep', '\\t']
tabl2sql.transform.main(args)

# benchmarking

file size | row count | time        | Hardware info                                                         | notes
7.5GB     | 8.3M rows | 2hrs 4min   | HP DL360 gen9 , RHEL 7.3 , 2 XEON E5-2643 v4 @ 3.4GHz , 64GB RAM      |
