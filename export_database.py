"""
Export PostgreSQL database to a single SQL file.

This script creates a complete backup of the airbnb_db database that can be
shared and restored on any PostgreSQL server.

Parameters
----------
None (uses environment variables from .env)

Returns
-------
None
    Creates a .sql file in the project directory

External Files
--------------
Input: .env file with database credentials
Output: airbnb_db_backup_YYYYMMDD_HHMMSS.sql

Environment Variables
---------------------
DB_HOST : str
    PostgreSQL server hostname (default: localhost)
DB_PORT : str
    PostgreSQL server port (default: 5432)
DB_NAME : str
    Database name to export (default: airbnb_db)
DB_USER : str
    PostgreSQL username (default: postgres)
DB_PASSWORD : str
    PostgreSQL password (required)
"""

import subprocess
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path


def export_database_to_sql():
    """
    Export PostgreSQL database using pg_dump utility.
    
    This function creates a complete database backup including:
    - Schema (table structures, constraints, indexes)
    - Data (all rows from all tables)
    - Sequences (auto-increment values)
    
    The output is a single SQL file that can be executed to recreate
    the entire database.
    
    Workflow
    --------
    1. Load database credentials from .env file
    2. Use hardcoded path to pg_dump executable
    3. Create timestamped backup filename
    4. Execute pg_dump command with credentials
    5. Verify backup file was created and report size
    
    Returns
    -------
    str
        Path to the created backup file
    
    Raises
    ------
    FileNotFoundError
        If pg_dump executable does not exist at specified path
    subprocess.CalledProcessError
        If pg_dump command fails
    ValueError
        If DB_PASSWORD is not set in .env file
    
    Examples
    --------
    >>> backup_file = export_database_to_sql()
    >>> print(f"Database exported to: {backup_file}")
    Database exported to: airbnb_db_backup_20251111_143022.sql
    
    Notes
    -----
    The backup file includes:
    - CREATE TABLE statements
    - INSERT statements for all data
    - ALTER TABLE statements for constraints
    - CREATE INDEX statements
    - Sequence current values
    
    The `--no-owner` and `--no-acl` flags ensure the backup can be
    restored on any PostgreSQL server without ownership conflicts.
    """
    load_dotenv()
    
    # Get database credentials from environment
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "airbnb_db")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    
    if not db_password:
        raise ValueError("DB_PASSWORD not found in .env file!")
    
    # Use hardcoded path to pg_dump.exe instead of searching for it
    pg_dump_path = r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"
    
    # Verify pg_dump exists at specified location
    if not Path(pg_dump_path).exists():
        raise FileNotFoundError(
            f"pg_dump.exe not found at: {pg_dump_path}\n\n"
            "Please verify:\n"
            "1. PostgreSQL 18 is installed\n"
            "2. Installation path is: C:\\Program Files\\PostgreSQL\\18\n"
            "3. Or update the pg_dump_path variable in this script"
        )
    
    # Create timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d")
    backup_file = f"airbnb_db_backup_{timestamp}.sql"
    
    print(f"🔄 Exporting database '{db_name}' to {backup_file}...")
    print(f"   Using: {pg_dump_path}")
    
    # Set password environment variable for pg_dump
    env = os.environ.copy()
    env['PGPASSWORD'] = db_password
    
    # Build pg_dump command
    cmd = [
        pg_dump_path,
        '-h', db_host,
        '-p', db_port,
        '-U', db_user,
        '-d', db_name,
        '-f', backup_file,
        '--verbose',
        '--no-owner',  # Don't output ownership commands
        '--no-acl'     # Don't output ACL commands
    ]
    
    try:
        # Run pg_dump
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Check file was created and get size
        backup_path = Path(backup_file)
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file was not created: {backup_file}")
        
        file_size = backup_path.stat().st_size / (1024 * 1024)  # Convert to MB
        
        print(f"✅ Database exported successfully!")
        print(f"   📁 File: {backup_file}")
        print(f"   📊 Size: {file_size:.2f} MB")
        
        return backup_file
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Export failed!")
        print(f"   Error: {e.stderr}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    try:
        backup_file = export_database_to_sql()
        
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        exit(1)