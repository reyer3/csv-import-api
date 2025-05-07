import os
import tempfile
import pandas as pd
import asyncio
import asyncpg
import asyncssh
import time
from io import StringIO
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logger
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection parameters from environment variables
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB = os.getenv("PG_DB")
PG_CONN_STRING = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=require"

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_PATH = os.getenv("SFTP_PATH")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))  # Number of rows per chunk

# Class to collect execution results
class ImportResult:
    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        self.downloaded_files = []
        self.processed_files = []
        self.status = "running"
        self.errors = []
        self.log_messages = []
        
    def log(self, level, message):
        """Add a log message and also send to logger"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} - {level} - {message}"
        self.log_messages.append(log_entry)
        
        if level == "ERROR":
            logger.error(message)
            self.errors.append(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)
    
    def complete(self, success=True):
        """Mark the import as complete"""
        self.end_time = datetime.now()
        self.status = "success" if success else "failed"
        
    def to_dict(self):
        """Convert results to dictionary"""
        duration = None
        if self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            
        return {
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration,
            "downloaded_files": self.downloaded_files,
            "processed_files": self.processed_files,
            "errors": self.errors,
            "row_counts": {file: count for file, count in self.processed_files},
            "log_messages": self.log_messages[-100:] if len(self.log_messages) > 100 else self.log_messages
        }

# Modified functions to use ImportResult for logging and tracking
async def download_files(download_dir, result):
    result.log("INFO", f"Downloading files from SFTP server {SFTP_HOST}...")
    
    try:
        # Connect to SFTP server
        async with asyncssh.connect(
            SFTP_HOST, 
            username=SFTP_USER, 
            password=SFTP_PASS,
            known_hosts=None
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                # Change to the remote directory
                await sftp.chdir(SFTP_PATH)
                
                # Get list of CSV files
                files = [f for f in await sftp.listdir() if f.endswith('.csv')]
                
                if len(files) != 3:
                    result.log("WARNING", f"Expected exactly 3 files, but found {len(files)}")
                
                result.log("INFO", f"Found files: {files}")
                
                # Download files concurrently
                download_tasks = []
                for file in files:
                    local_path = os.path.join(download_dir, file)
                    download_tasks.append(sftp.get(file, local_path))
                
                # Wait for all downloads to complete
                await asyncio.gather(*download_tasks)
                
                result.log("INFO", f"Successfully downloaded {len(files)} files")
                result.downloaded_files = files
                return files
    
    except Exception as e:
        result.log("ERROR", f"Error downloading files: {e}")
        return []

async def get_table_schema(conn, table_name, result):
    # Get column information
    schema_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = $1 
        ORDER BY ordinal_position
    """
    
    columns = await conn.fetch(schema_query, table_name)
    
    # Convert to tuple format for compatibility
    schema = [(col['column_name'], col['data_type']) for col in columns]
    
    result.log("INFO", f"Schema for {table_name}: {schema}")
    return schema

async def process_dataframe(conn, df, table_name, schema, result):
    # Prepare column lists
    db_columns = [col[0] for col in schema]
    
    # Match available columns from dataframe to database schema
    matched_columns = []
    for db_col in db_columns:
        # Find a matching column in DataFrame (case insensitive)
        match = None
        for df_col in df.columns:
            if df_col.lower() == db_col.lower():
                match = df_col
                break
        
        if match:
            matched_columns.append((db_col, match))
        else:
            # Special handling for loaded_at column
            if db_col == 'loaded_at':
                matched_columns.append((db_col, None))  # Will handle special later
            else:
                result.log("WARNING", f"Column {db_col} not found in CSV")
    
    if not matched_columns:
        result.log("ERROR", "No columns matched between CSV and database table")
        return False
    
    # Prepare SQL parts
    db_cols_sql = ', '.join([f'"{col[0]}"' for col in matched_columns])
    placeholders = ', '.join([f'${i+1}' for i in range(len(matched_columns))])
    
    # Prepare data insert query
    insert_query = f'INSERT INTO "{table_name}" ({db_cols_sql}) VALUES ({placeholders})'
    
    # Create a prepared statement for better performance
    prepared_stmt = await conn.prepare(insert_query)
    
    # Get current timestamp for loaded_at
    current_timestamp = datetime.now()
    
    # Insert data in batches
    batch_size = 1000
    rows_inserted = 0
    
    # Process in batches
    total_rows = len(df)
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_data = []
        
        # Process each row in the batch
        for _, row in df.iloc[start_idx:end_idx].iterrows():
            row_data = []
            for db_col, df_col in matched_columns:
                # Find data type for this column
                col_type = next(schema_col[1] for schema_col in schema if schema_col[0] == db_col)
                
                # Special handling for loaded_at
                if db_col == 'loaded_at' and df_col is None:
                    row_data.append(current_timestamp)
                    continue
                
                value = row[df_col] if df_col is not None else None
                
                # Handle type conversions based on PostgreSQL data type
                if pd.isna(value) or value == '':
                    row_data.append(None)
                
                # Handle integer types
                elif col_type in ('integer', 'bigint', 'smallint'):
                    try:
                        row_data.append(int(value))
                    except (ValueError, TypeError):
                        row_data.append(None)
                
                # Handle numeric types
                elif col_type in ('numeric', 'decimal', 'real', 'double precision'):
                    try:
                        row_data.append(float(value))
                    except (ValueError, TypeError):
                        row_data.append(None)
                
                # Handle date type
                elif col_type == 'date':
                    try:
                        if isinstance(value, str):
                            # Parse date string into date object
                            row_data.append(datetime.strptime(value, '%Y-%m-%d').date())
                        else:
                            row_data.append(value)
                    except (ValueError, TypeError):
                        row_data.append(None)
                
                # Handle timestamp types
                elif 'timestamp' in col_type:
                    try:
                        if isinstance(value, str):
                            # Parse timestamp string
                            row_data.append(datetime.strptime(value, '%Y-%m-%d %H:%M:%S'))
                        else:
                            row_data.append(value)
                    except (ValueError, TypeError):
                        row_data.append(None)
                
                # All other types (strings, etc.)
                else:
                    row_data.append(value)
            
            batch_data.append(tuple(row_data))
        
        # Execute the batch
        if batch_data:
            try:
                await prepared_stmt.executemany(batch_data)
                rows_inserted += len(batch_data)
                result.log("INFO", f"Inserted batch {start_idx}-{end_idx} of {total_rows} rows")
            except Exception as e:
                result.log("ERROR", f"Error inserting batch: {e}")
                # Continue with next batch anyway
    
    result.log("INFO", f"Inserted {rows_inserted} rows into {table_name}")
    return rows_inserted

async def import_data(csv_file, table_name, download_dir, pool, result):
    result.log("INFO", f"Processing {csv_file} into table {table_name}...")
    
    # Get a connection from the pool
    async with pool.acquire() as conn:
        # Truncate the target table
        try:
            result.log("INFO", f"Truncating table {table_name}...")
            await conn.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')
        except Exception as e:
            result.log("WARNING", f"Could not truncate table: {e}")
        
        # Get table schema
        schema = await get_table_schema(conn, table_name, result)
        if not schema:
            result.log("ERROR", f"Could not retrieve schema for table {table_name}")
            return 0
        
        try:
            # Read CSV with pandas - handle encodings and BOM characters
            filepath = os.path.join(download_dir, csv_file)
            file_size = os.path.getsize(filepath) / (1024 * 1024)  # Size in MB
            
            result.log("INFO", f"Reading CSV file {csv_file} (size: {file_size:.1f} MB)")
            
            # Try different encodings if needed
            try:
                df = pd.read_csv(filepath, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(filepath, encoding='latin1')
                except:
                    df = pd.read_csv(filepath, encoding='utf-8-sig')  # Handle BOM
            
            # Clean column names (remove any invisible characters)
            df.columns = [col.strip().replace('\ufeff', '') for col in df.columns]
            
            rows_imported = 0
            
            # Check if file needs to be processed in chunks
            if file_size > 10:  # If file is larger than 10MB
                result.log("INFO", f"Large file detected, processing in chunks of {CHUNK_SIZE} rows")
                total_rows = len(df)
                
                # Process in chunks
                for start in range(0, total_rows, CHUNK_SIZE):
                    end = min(start + CHUNK_SIZE, total_rows)
                    result.log("INFO", f"Processing chunk (rows {start}-{end})")
                    chunk_df = df.iloc[start:end].copy()
                    
                    # Process this chunk and wait for it to complete
                    chunk_rows = await process_dataframe(conn, chunk_df, table_name, schema, result)
                    rows_imported += chunk_rows
            else:
                # Process the entire dataframe
                rows_imported = await process_dataframe(conn, df, table_name, schema, result)
            
            result.log("INFO", f"✅ Completed import of {csv_file}")
            return rows_imported
            
        except Exception as e:
            result.log("ERROR", f"Error processing {csv_file}: {e}")
            import traceback
            result.log("ERROR", traceback.format_exc())
            return 0

async def run_import():
    """Main import function that can be called from API"""
    result = ImportResult()
    result.log("INFO", "Starting CSV import process...")
    
    # Create a temporary directory for downloads
    download_dir = tempfile.mkdtemp()
    result.log("INFO", f"Created temporary directory: {download_dir}")
    
    try:
        # Download files from SFTP
        files = await download_files(download_dir, result)
        if not files:
            result.log("ERROR", "No files were downloaded. Exiting.")
            result.complete(False)
            return result
        
        # Create connection pool
        result.log("INFO", "Creating database connection pool...")
        pool = await asyncpg.create_pool(PG_CONN_STRING)
        
        # Import files (process each file one at a time to avoid database conflicts)
        for csv_file in files:
            # Extract table name from filename
            table_name = os.path.splitext(csv_file)[0]
            rows_imported = await import_data(csv_file, table_name, download_dir, pool, result)
            result.processed_files.append((csv_file, rows_imported))
        
        # Close the connection pool
        await pool.close()
        
        result.log("INFO", "All imports completed!")
        result.complete(True)
        
    except Exception as e:
        result.log("ERROR", f"Unexpected error: {e}")
        import traceback
        result.log("ERROR", traceback.format_exc())
        result.complete(False)
    finally:
        try:
            result.log("INFO", "Cleaning up temporary directory...")
            # Close any open file handles first
            import gc
            gc.collect()
            # Wait a bit for Windows to release any file locks
            time.sleep(1)
            
            # Try to remove the directory, but don't crash if it fails
            try:
                for root, dirs, files in os.walk(download_dir):
                    for f in files:
                        os.unlink(os.path.join(root, f))
                    for d in dirs:
                        os.rmdir(os.path.join(root, d))
                os.rmdir(download_dir)
                result.log("INFO", "Temporary directory cleaned up")
            except Exception as e:
                result.log("WARNING", f"Could not completely clean up temporary directory: {e}")
        except Exception as e:
            result.log("WARNING", f"Error during cleanup: {e}")
    
    return result
