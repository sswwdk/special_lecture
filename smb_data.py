import os
import csv
import mariadb
import sys

# --- Database Connection Details (Please update with your credentials) ---
# Replace with your MariaDB username
DB_USER = "your_username"
# Replace with your MariaDB password
DB_PASSWORD = "your_password"
# Replace with your MariaDB host (e.g., 'localhost' or an IP address)
DB_HOST = "localhost"
# Replace with your MariaDB port (default is 3306)
DB_PORT = 3306  # Note: mariadb connector uses an integer for port
# The name of the database you are using
DB_NAME = "cp_data"
# The name of the target table
TABLE_NAME = "tb_smb_ods"

# --- CSV File Location ---
# IMPORTANT: Replace with the absolute or relative path to the folder containing your CSV files.
# Example for Windows: 'C:/Users/YourUser/Documents/CSVs'
# Example for macOS/Linux: '/home/user/data/csvs'
CSV_DIRECTORY = "C:/data/smb_data"


def load_csvs_to_mariadb_direct():
    """
    Scans a directory for CSV files and loads their content into a MariaDB table
    using the native mariadb connector and row-by-row insertion.
    """
    print("Starting the CSV to MariaDB loading process...")

    # --- 1. Establish Database Connection ---
    try:
        conn = mariadb.connect(
            user= "lguplus7",
            password= "lg7p@ssw0rd~!",
            host="localhost",
            port=3310,
            database="cp_data"
        )
        print("Successfully connected to MariaDB.")
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        sys.exit(1)

    # Get a cursor for executing queries
    cur = conn.cursor()

    # --- 2. Prepare the INSERT Statement ---
    # Your table has 39 columns. We create a placeholder for each one.
    # e.g., "INSERT INTO tb_smb_ods VALUES (?, ?, ..., ?)"
    placeholders = ", ".join(["?"] * 39)
    insert_sql = f"INSERT INTO {TABLE_NAME} VALUES ({placeholders})"
    print(f"Using SQL statement: {insert_sql}")

    # --- 3. Iterate Through CSV Files and Load Data ---
    try:
        files_in_directory = os.listdir(CSV_DIRECTORY)
        csv_files = [f for f in files_in_directory if f.endswith('.csv')]

        if not csv_files:
            print(f"No CSV files found in the directory: {CSV_DIRECTORY}")
            return

        print(f"Found {len(csv_files)} CSV files to process.")

        # Process each CSV file
        for filename in csv_files:
            file_path = os.path.join(CSV_DIRECTORY, filename)
            print(f"Processing file: {file_path}...")
            
            try:
                # Open the CSV file.
                # We specify the encoding as 'utf-8'. If you encounter errors,
                # you might need to change it to 'cp949' or 'euc-kr' for Korean files.
                with open(file_path, mode='r', encoding='utf-8') as csvfile:
                    csv_reader = csv.reader(csvfile)
                    
                    # --- MODIFICATION START ---
                    # Skip the header row so it is not inserted into the database.
                    next(csv_reader, None)
                    # --- MODIFICATION END ---

                    rows_inserted = 0
                    for row in csv_reader:
                        # Ensure the row has exactly 39 columns to match the table structure
                        if len(row) == 39:
                            cur.execute(insert_sql, tuple(row))
                            rows_inserted += 1
                        else:
                            print(f"  [Warning] Skipping row with {len(row)} columns in {filename}: {row}")
                
                # Commit the transaction after successfully processing the entire file.
                # This is more efficient than committing after every single row.
                conn.commit()
                print(f"Successfully processed {rows_inserted} rows from {filename} and committed to the database.")

            except FileNotFoundError:
                print(f"  [Error] The file {filename} was not found.")
            except Exception as e:
                print(f"  [Error] An error occurred while processing {filename}: {e}")
                print("  Skipping this file and continuing with the next one.")

        print("\nAll CSV files have been processed.")

    except FileNotFoundError:
        print(f"Error: The directory '{CSV_DIRECTORY}' was not found. Please check the path.")
    except Exception as e:
        print(f"An unexpected error occurred during file processing: {e}")
    finally:
        # --- 4. Close the Connection ---
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    load_csvs_to_mariadb_direct()
