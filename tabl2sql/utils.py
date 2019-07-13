import sys
from datetime import datetime
import glob

ord_list = list(range(48,57,1)) + list(range(65,90,1)) + [95] + list(range(97,122,1))


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
