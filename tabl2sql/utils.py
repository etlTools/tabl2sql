
# python imports
import argparse
import sys
from datetime import datetime
import glob

ord_list = list(range(48,57,1)) + list(range(65,90,1)) + [95] + list(range(97,122,1))


def parse_args(args):
    """provide args breakdown for option to use __main__.py or transform.main()

    Parameters
    ----------
    -files : string
        file(s) to be ripped
    -dirs : string
        director(y,ies) to pull files from 
    -table : string
        name of destination table in database. if exists, to_sql_mode must == 'append'. 
        default: function will create one at runtime
    -mode : string
        to_sql_mode, read pd.to_sql docs
        default: `fail`
    -sql : string
        flavor of sql, such as mysql
    -driver : string
        driver to be used, must be installed before, such as mysqlconnector
    -user : string
        name in db that has create & update table permission in destination db
    -pw : string
        user's password
    -host : string
        host & port to use for db
        defaul: `localhost`
    -db : string
        name of destination db
    -sep : string
        define separator used in .txt when not csv
    -encoding : string
        read pd.read_csv docs
    """
    parser = argparse.ArgumentParser(
        description='arguments to provide to transform.main()',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-files', dest="filenames", default=[], nargs="+" )
    parser.add_argument('-dirs', dest="dirs", default=[], nargs="+" )
    parser.add_argument('-table', dest="table", default='py_imp_{}'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))
    parser.add_argument('-mode', default='fail')
    parser.add_argument('-sql')
    parser.add_argument('-driver')
    parser.add_argument('-user')
    parser.add_argument('-pw')
    parser.add_argument('-host', default='@localhost', help='convention:`@host:port`')
    parser.add_argument('-db')
    parser.add_argument('-sep')
    parser.add_argument('-encoding')

    return parser.parse_args(args)


def getfilesfromdir(dirs: list, recurs: bool =True):
    """collect csv & txt file absolute paths for handoff to transform.populate_df, with option for recursive

    Parameters
    ----------
    dirs : list
        A list of directories to be searched for data files
    recurs : boolean
        option for whether file search will be recursive

    Returns
    ----------
    files: list 
        A list of files within specified directories to be handed off to transform.populate_df
    """
    
    all_files = []
    for dir in dirs:
        all_files.extend(glob.glob(dir+ '**/*', recursive=recurs))
    # all_files = [inner for outer in list_of_lists for inner in outer] # obviated by extend ilo append above
    exts = ['.txt', '.csv']
    files = [file for file in all_files if any([ext for ext in exts if file.endswith(ext)])]

    return sorted(files)
