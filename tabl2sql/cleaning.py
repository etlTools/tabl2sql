import logging
import pandas as pd
import re
log = logging.getLogger(__name__)

# project imports
from tabl2sql import utils

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
    
    # cleaning blanks and whitespace
    input_df.applymap(lambda x: np.nan if isinstance(x, str) and (x.isspace() or not x) else x)
    # cleaning unicode out of entire dataframe
    input_df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)

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
    
    input_df.columns = input_df.columns.str.strip()
    input_df.columns = input_df.columns.str.replace(' ', '_')
    input_df.columns = input_df.columns.str.lower()
    for j in range(len(input_df.columns.values)):
        input_df.columns.values[j] = "".join(i for i in input_df.columns.values[j] if ord(i) in utils.ord_list)
    # make duplicate column names unique
    input_df.columns = pd.io.parsers.ParserBase({'names':input_df.columns})._maybe_dedup_names(input_df.columns)
    # these are reserved oracle words
    input_df.rename(columns={'type':'type_', 'group':'group_', 'date': 'date_', 'resource':'resource_',
                           'start':'start_', 'end':'end_'}, inplace=True)
    
    return input_df
    
    
def to_date(input_df):
    """try to convert any columns with 'dt' or 'date' in name or with a regex date in [0] to datetime

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
        if any([piece for piece in ['dt', 'date'] if re.match('^' + piece + '|' + piece + '$', col.lower())])\
                or re.match('(\d{1,4})[^0-9a-zA-Z](\d{1,4})[^0-9a-zA-Z](\d{1,4})', str(input_df[col][0])):
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
        DataFrame with object fields converted to string
        
    dtype_dict: dictionary: 
        A dictionary for df.to_sql with dtypes and lengths for columns that have been converted to string
    """
    
    log.info("attempting to fix dates")
    for col in input_df.columns:
        if input_df[col].dtype == 'object':
            input_df[col] = input_df[col].astype(str)
            dtype_dict[col] = String(input_df[col].apply(str).map(len).max())
    try:
        log.info("\nlist of string conversions: \n{}".format(json.dumps(dtype_dict, indent=2)))
    except:
        log.info("\nlist of string conversions: \n{}".format(dtype_dict))
    
    return input_df, dtype_dict
