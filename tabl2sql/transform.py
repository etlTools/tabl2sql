
# python imports
import argparse
import pandas as pd
import numpy as np
import math
import time
from datetime import datetime, timedelta
import sys
import os
import re
import json
from sqlalchemy.types import String
import logging
log = logging.getLogger(__name__)

# project imports
import utils


def populate_df(self, filenames: list, seperator: str =','):
    """populate DataFrame for handoff to load_data()

    Parameters
    ----------
    filenames : list
        A list of files to be imported, output of utils.getfilesfromdir() can be used
    separator : string
        define separator used in .txt when not csv

    Returns
    ----------
    staging_df: pd.DataFrame 
        dataframe populated with content from imported files
    """
    
    start_time = time.time()
    
    file_count = 0
    total_files = len(filenames)
    inp_cols = []

    log.info("Coalescing/preparing files:\n\t{} ".format(filenames))
    
    for filename in filenames:
        dtype_dict = dict()
        file_count += 1
        read_df = pd.read_csv(r"{}".format(filename), sep=seperator, encoding='cp1252')

        # setup column list & df
        if file_count == 1:
            inp_cols = list(read_df.columns.values)
            staging_df = read_df.copy()
            log.info("\n\t created df with {} rows".format(staging_df.shape[0]))
    
        # check for new columns
        else:
            log.info("\n checking for new columns in {} of {}: {}".format(file_count, total_files, filename))
            for col in read_df.columns:
                if col not in inp_cols:
                    utils.write_err("*** adding column {} ***".format(col))
                    inp_cols.append(col)
            staging_df = staging_df.append(read_df, ignore_index=True)
            log.info("\n\t appending {} rows, totalling {}".format(read_df.shape[0], staging_df.shape[0]))
            
    log.info("\n\ndf info: \n{}".format(staging_df.info(verbose=True)))
    log.info("\ndf length: \n{}".format(staging_df.shape[0]))
    log.info("\ndf head: \n{}".format(staging_df.head()))
    log.info('df populated in {}'.format(timedelta(seconds=seco)))
    
    return staging_df


def load_data(self, load_df, db_engine, to_sql_mode='fail', dest_table='tabl2sql_{}'.format(datetime.now().strftime('%Y%m%d_%H%M%S')), dtype_dict={}):
    """receive df from populate_df() & load to db 

    Parameters
    ----------
    load_df : pd.DataFrame
        DataFrame to load to db
    db_engine : sqlalchemy engine
        define separator used in .txt when not csv
    to_sql_mode : string
        read pd.to_sql docs
    dest_table : string
        name of destination table in database. if exists, to_sql_mode must == 'append'. if not provided, function will create one
    dtype_dict : dictionary
        oracle defaults strings to clobs; 2nd output of cleaning.avoid_clob() --dtype_dict-- can be used to force varchar & determine field length

    Notes
    -----
    user must create engine using sqlalchemy & provide
    """
    start_time = time.time()
    if load_df.shape[0] < 50000:
        load_df.to_sql(dest_table, db_engine, if_exists=to_sql_mode, dtype=dtype_dict, index=False)
    else:
        num_loops = math.ceil(load_df.shape[0]/50000)
        partial_df = load_df.iloc[0:50000]
        partial_df.to_sql(dest_table, db_engine, if_exists=to_sql_mode, dtype=dtype_dict, index=False)
        for loop in range(1, num_loops):
            partial_df = load_df.iloc[loop*50000+1:(loop+1)*50000+1]
            partial_df.to_sql(dest_table, engines[pargs.db], if_exists='append', dtype=dtype_dict, index=False)
            log.info("\n\loaded {} lines".format((loop+1)*50000))

    seco = int(time.time() - start_time)
    log.info('loading completed in {}'.format(timedelta(seconds=seco)))


def load_test(self, test_df):
    num_loops = test_df.shape[0] 
    for loop in range(1,num_loops):
        partial_df = pd.DataFrame(test_df.iloc[loop]) 
        try:
            partial_df.to_sql(dest_table, db_engine, if_exists='append', dtype=dtype_dict, index=False)
        except:
            log.error("row {}: {} \n\t {}".format(loop, test_df.iloc[loop], test_df.iloc[loop].dtypes))
