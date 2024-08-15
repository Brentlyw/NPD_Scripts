import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import csv

def ext_me_recs_dask(in_file, out_file, mode='w'):
    dtypes = {
        'ID': str,
        'fname': str,
        'lname': str,
        'mname': str,
        'suff': str,
        'dob': str,
        'addr': str,
        'cty': str,
        'cnty': str,
        'st': str,
        'zip': str,
        'ph1': str,
        'aka1': str,
        'aka2': str,
        'aka3': str,
        'start': str,
        'alt_dob1': str,
        'alt_dob2': str,
        'alt_dob3': str,
        'ssn': str
    }
    
    try:
        df = dd.read_csv(in_file, usecols=list(dtypes.keys()), dtype=dtypes, encoding_errors='ignore', quoting=csv.QUOTE_ALL)
        filtered_df = df[df['st'] == 'AL'] #YOUR STATE ABBREV HERE (Also change output)
        with ProgressBar():
            filtered_df.to_csv(out_file, single_file=True, index=False, mode=mode, header=(mode == 'w'))
    except Exception as err:
        print(f"Err: {err}. Skipping.")

# Process each file and append results to ME.csv
ext_me_recs_dask('ssn.csv', 'AL.csv', mode='w')  
ext_me_recs_dask('ssn2.csv', 'AL.csv', mode='a') #append mode
