import logging
import os
import yaml
import re
import dask.dataframe as dd

################
# File Reading #
################

def read_config_file(filepath):
    '''
    Read YAML data configuration file
    '''
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def read_data(table_config):
    '''
    Read csv file
    '''
    file_name = table_config['file_name']
    file_type = table_config['file_type']
    file = file_name + f'.{file_type}'

    delimiter = table_config['inbound_delimiter']

    df = dd.read_csv(file, delimiter=delimiter)
    return df


def replacer(string, char):
    '''
    Reaplce duplicated character
    '''
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string


def col_header_val(df,table_config):
    '''
    Replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return True
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return False


def print_summary(df, output_file):
    '''
    Print output file summary 
    '''

    filesize_gb = os.path.getsize(output_file)/ 1024 ** 3
    number_of_cols = len(df.columns)
    number_of_rows = len(df)

    print('Summary:')
    print('File size: {:.1f} Gb'.format(filesize_gb))
    print('Number of columns: {}'.format(number_of_cols))
    print('Number of rows: {}'.format(number_of_rows))


def write_compressed(df, table_config, compression='gzip'):
    '''
    Write file in comppressed gzip format
    '''

    output_file = table_config['file_name'] + '.gz'
    sep = table_config['outbound_delimiter']

    df.to_csv(output_file, 
          compression=compression, 
          sep=sep, 
          single_file=True,
          index=False
         )
    
    print_summary(df, output_file)


