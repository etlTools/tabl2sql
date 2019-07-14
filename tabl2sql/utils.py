import argparse
import sys
from datetime import datetime
import glob

ord_list = list(range(48,57,1)) + list(range(65,90,1)) + [95] + list(range(97,122,1))


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='transform CSV to oracle database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-files', dest="filenames", default=[], nargs="+" )
    parser.add_argument('-dirs', dest="dirs", default=[], nargs="+" )
    parser.add_argument('-table', dest="table", default='py_imp_{}'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))
    parser.add_argument('-mode', default='fail')
    parser.add_argument('-db')
    parser.add_argument('-sep', default=',')

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
    
    list_of_lists = glob.glob(dir+ '**/*', recursive=recurs)
    all_files = [inner for outer in list_of_lists for inner in outer]
    exts = ['.txt', '.csv']
    files = [file for file in all_files if any([ext for ext in exts if file.endswith(piece)])]

    return sorted(files)
