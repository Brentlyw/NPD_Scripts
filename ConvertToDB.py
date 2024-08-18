import os
import sqlite3
import pandas as pd
from tqdm import tqdm

def convert_csv_to_sqlite(folder_path, chunk_size=100000):
    # Get a list of all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    for csv_file in tqdm(csv_files, desc="Converting CSVs to SQLite"):
        csv_path = os.path.join(folder_path, csv_file)
        db_name = os.path.splitext(csv_file)[0] + '.db'
        db_path = os.path.join(folder_path, db_name)
        
        # Create a connection to the SQLite database
        conn = sqlite3.connect(db_path)
        
        # Get the total number of lines in the CSV file for the progress bar
        total_lines = sum(1 for _ in open(csv_path, 'r')) - 1  # Subtract 1 for header
        chunks = pd.read_csv(csv_path, dtype=str, chunksize=chunk_size)
        
        with tqdm(total=total_lines, desc=f"Processing {csv_file}", unit='lines') as pbar:
            for chunk in chunks:
                chunk.to_sql(name='data', con=conn, if_exists='append', index=False)
                pbar.update(chunk.shape[0])
        
        conn.close()

if __name__ == "__main__":
    folder_path = input("Enter the folder path containing the .csv files: ")
    convert_csv_to_sqlite(folder_path)
