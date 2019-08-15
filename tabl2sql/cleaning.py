
# python imports
import pandas as pd
import re
import json
import numpy as np
from sqlalchemy.types import String
import dateutil
import logging
log = logging.getLogger(__name__)

# project imports
from . import utils

def clean_data(input_df: pd.DataFrame):
    """Clean data within dataframe to prepare for SQL

    Parameters
    ----------
    input_df : pd.DataFrame
        a pandas dataframe

    Returns
    ----------
    input_df: pd.DataFrame: 
        cleaned DataFrame 
    """
    # clean unicode & whitespace
    input_df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
    input_df = input_df.applymap(lambda x: np.nan if pd.isnull(x) else \
                    (np.nan if isinstance(x, str) and x.isspace() else str(x)))
    return input_df


def clean_cols(input_df: pd.DataFrame):
    """Clean dataframe column names to prepare for SQL - originally developed within Oracle's reqs

    Parameters
    ----------
    input_df : pd.DataFrame
        a pandas dataframe

    Returns
    ----------
    input_df: pd.DataFrame: 
        DataFrame with cleaned & deduplicated column names
    """
    # remove whitespace, replace with underscores
    input_df.columns = input_df.columns.str.strip()
    input_df.columns = input_df.columns.str.replace(' ', '_')
    input_df.columns = input_df.columns.str.lower()
    # leave only letters, numbers and underscores
    for j in range(len(input_df.columns.values)):
        input_df.columns.values[j] = "".join(i for i in input_df.columns.values[j] if ord(i) in utils.ord_list)
    # make duplicate column names unique
    input_df.columns = pd.io.parsers.ParserBase({'names':input_df.columns})._maybe_dedup_names(input_df.columns)
    # these are reserved oracle words
    input_df.rename(columns={'type':'type_', 'group':'group_', 'date': 'date_', 'resource':'resource_',
                           'start':'start_', 'end':'end_', 'sysdate':'sysdate_'}, inplace=True)
    
    return input_df
    
    
def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date. credit to Alex Riley, https://stackoverflow.com/a/25341965

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        dateutil.parser.parse(string, fuzzy=fuzzy)
        if re.match('(?:\d{1}|\d{2}|\d{4})[^0-9a-zA-Z](\d{1,4})[^0-9a-zA-Z](?:\d{1}|\d{2}|\d{4})', string):
            return True
    
    except ValueError:
        return False
    
    
def to_date(input_df):
    """try to convert any columns with 'dt' or 'date' at beginning or end of name or with a regex date in [0] to datetime

    Parameters
    ----------
    input_df : pd.DataFrame
        a pandas dataframe

    Returns
    ----------
    input_df: pd.DataFrame: 
        DataFrame with recognized date columns converted to datetime
    """
    
    log.info("attempting to fix dates")
    for col in input_df.columns:
        if any([piece for piece in ['dt', 'date'] if re.match('^{0}|.*{0}$'.format(piece), col.lower())])\
                or (is_date(str(input_df[col][0]).strip())):
            input_df[col] = pd.to_datetime(input_df[col], infer_datetime_format=True, errors='coerce')
            log.info("Attempted to correct {} to datetime - did it work? {}\n"
                  .format(col, input_df[col].dtype.kind == 'M'))  # 'M' is numpy dtype for datetime
    log.info("done fixing dates")
    
    return input_df

    
def avoid_clob(input_df: pd.DataFrame):
    """convert objects to strings to prepare for varchar because to_sql defaults to creating clobs in oracle

    Parameters
    ----------
    input_df : pd.DataFrame
        a pandas dataframe

    Returns
    ----------
    input_df: pd.DataFrame: 
        DataFrame with object fields converted to string & replaced empties and whitespace with nan
        
    dtype_dict: dictionary: 
        A dictionary for df.to_sql with dtypes and lengths for columns that have been converted to string
    """
    
    dtype_dict = dict()
    log.info("building string dict")
    for col in input_df.columns:
        if input_df[col].dtype == 'object':
            char_len = input_df[col].apply(str).map(len).max()
            if char_len > 4000:
                log.info("sorry bucko, {} is stuck as clob length {}".format(col, char_len))
            else:
                dtype_dict[col] = String(char_len)
    try:
        log.info("\nlist of string conversions: \n{}".format(json.dumps(dtype_dict, indent=2)))
    except:
        log.info("\nlist of string conversions: \n{}".format(dtype_dict))
    
    return input_df, dtype_dict
