import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import csv

def extract_state_records_dask(input_file, states, mode='w'):
    dtype = {
        'ID': str,
        'firstname': str,
        'lastname': str,
        'middlename': str,
        'name_suff': str,
        'dob': str,
        'address': str,
        'city': str,
        'county_name': str,
        'st': str,
        'zip': str,
        'phone1': str,
        'aka1fullname': str,
        'aka2fullname': str,
        'aka3fullname': str,
        'StartDat': str,
        'alt1DOB': str,
        'alt2DOB': str,
        'alt3DOB': str,
        'ssn': str
    }
    
    try:
        # Read the CSV file with proper handling of quoted fields
        df = dd.read_csv(input_file, usecols=list(dtype.keys()), dtype=dtype, encoding_errors='ignore', quoting=csv.QUOTE_ALL)
        
        # Process each state
        for state in states:
            print(f"Processing state: {state}")
            
            # Filter the dataframe for the current state
            filtered_df = df[df['st'] == state]
            
            # Save the filtered results to the corresponding state CSV file
            output_file = f'{state}.csv'
            with ProgressBar():
                filtered_df.to_csv(output_file, single_file=True, index=False, mode=mode, header=(mode == 'w'))
    except Exception as e:
        print(f"An error occurred: {e}. Skipping problematic data and continuing.")

# List of all state abbreviations
states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
          'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
          'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
          'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
          'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

# Process ssn.csv first, writing new files
extract_state_records_dask('ssn.csv', states, mode='w')  # Process the first file

# Process ssn2.csv, appending to existing state files
extract_state_records_dask('ssn2.csv', states, mode='a') # Process the second file
