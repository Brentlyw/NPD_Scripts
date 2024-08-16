import pandas as pd
import os
from tqdm import tqdm

# Paths to your CSV files
csv_files = ['ssn.csv', 'ssn2.csv']

# Create a directory for the output if it doesn't exist
output_dir = 'statewise_output'
os.makedirs(output_dir, exist_ok=True)

# Define the chunk size (number of rows per chunk)
chunk_size = 10**6  # Adjust the chunk size as needed

# Process each CSV file
for csv_file in csv_files:
    # Initialize tqdm for progress tracking with dynamic chunk/s reporting
    progress_bar = tqdm(desc=f'Processing {csv_file}', unit='chunk')

    # Read the CSV file in chunks, handling bad lines
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, on_bad_lines='skip'):
        # Group data by the 'st' (state) column and write each group to a separate CSV
        grouped = chunk.groupby('st')

        for state, group in grouped:
            state_file = os.path.join(output_dir, f'{state}.csv')
            # Append to the state-specific file
            group.to_csv(state_file, mode='a', index=False, header=not os.path.exists(state_file))
        
        # Update the progress bar
        progress_bar.update(1)
        progress_bar.set_postfix(chunks_per_s=f'{progress_bar.format_dict["rate"]:.2f}')

    # Close the progress bar
    progress_bar.close()

print("Separation completed.")
