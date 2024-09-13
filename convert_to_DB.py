'''convert_to_DB.py'''

import os
import sqlite3
import pandas as pd

def create_sql_tables_from_csv(file_list, db_name='D:\\Git_repos\\bos\\cows.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    for file_path in file_list:
        # Extract filename from the file path
        file_name = os.path.basename(file_path)
        table_name = os.path.splitext(file_name)[0]  # Extract table name from filename without extension
        
        df = pd.read_csv(file_path)
        
        # Define columns for the table
        columns = ', '.join([f'`{col}` TEXT' for col in df.columns])
        
        # Create table if not exists
        create_table_sql = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})' 
        cursor.execute(create_table_sql)
        
        # Insert data into the table
        df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Commit changes and close connection
    conn.commit()
    conn.close()

# Example usage:
file_list = [
    'F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.csv',
    'F:\\COWS\\data\\insem_data\\all.csv'
]
create_sql_tables_from_csv(file_list)

