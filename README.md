# CSV Import API

API service for importing CSV data from SFTP to PostgreSQL

## Overview

This service provides a REST API for triggering and monitoring CSV imports from an SFTP server to a PostgreSQL database. It runs as a containerized application using Docker and handles large files, data type conversions, and error recovery.

## Features

- REST API for triggering imports
- Background processing for long-running imports
- Status monitoring and detailed logs
- Handles large CSV files by processing in chunks
- Automatic type conversion based on database schema
- Error recovery and detailed logging

## Prerequisites

- Docker and Docker Compose
- SFTP server with CSV files
- PostgreSQL database with existing tables

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/reyer3/csv-import-api.git
   cd csv-import-api
   ```

2. Create a `.env` file with your configuration (use `.env.example` as a template):
   ```bash
   cp .env.example .env
   ```

3. Edit the `.env` file with your SFTP and PostgreSQL credentials:
   ```
   # Database connection
   PG_HOST=your-db-host.example.com
   PG_PORT=5432
   PG_USER=your_db_username
   PG_PASS=your_db_password
   PG_DB=your_database_name

   # SFTP connection
   SFTP_HOST=your-sftp-server.com
   SFTP_USER=your_sftp_username
   SFTP_PASS=your_sftp_password
   SFTP_PATH=/path/to/csv/files
   
   # Application settings
   CHUNK_SIZE=10000
   ```

4. Build and start the container:
   ```bash
   docker-compose up -d
   ```

## API Usage

The API provides the following endpoints:

### Start an Import

```
POST /import
```

Response:
```json
{
  "job_id": "8f1e5efa-9c7d-4873-b7c4-3a8e65b3e724",
  "status": "starting",
  "start_time": "2025-05-07T12:34:56.789012",
  "log_lines": 0
}
```

### Check Import Status

```
GET /import/{job_id}
```

Response:
```json
{
  "job_id": "8f1e5efa-9c7d-4873-b7c4-3a8e65b3e724",
  "status": "success",
  "start_time": "2025-05-07T12:34:56.789012",
  "end_time": "2025-05-07T12:36:58.123456",
  "duration_seconds": 121.334444,
  "downloaded_files": [
    "bd_all_projects.csv",
    "bd_project_units.csv",
    "bd_all_images_project.csv"
  ],
  "processed_files": [
    ["bd_all_projects.csv", 755],
    ["bd_project_units.csv", 36135],
    ["bd_all_images_project.csv", 9686]
  ],
  "errors": [],
  "row_counts": {
    "bd_all_projects.csv": 755,
    "bd_project_units.csv": 36135,
    "bd_all_images_project.csv": 9686
  },
  "log_lines": 42
}
```

### Get Import Logs

```
GET /import/{job_id}/logs
```

Response:
```json
{
  "logs": [
    "2025-05-07 12:34:56 - INFO - Starting CSV import process...",
    "2025-05-07 12:34:56 - INFO - Created temporary directory: /tmp/tmp1234abcd",
    "2025-05-07 12:34:57 - INFO - Downloading files from SFTP server...",
    "...more log entries..."
  ]
}
```

## API Documentation

When the service is running, you can access the OpenAPI documentation at:

```
http://localhost:8000/docs
```

## Database Schema Requirements

The service expects the following:

1. Tables must already exist in the database
2. Table names must match CSV filenames (without the .csv extension)
3. Column names in the database should match headers in the CSV files

## Example Command-Line Usage

Using curl to trigger a new import:

```bash
curl -X POST http://localhost:8000/import
```

Get the status of a running import:

```bash
curl http://localhost:8000/import/8f1e5efa-9c7d-4873-b7c4-3a8e65b3e724
```

View the logs of an import:

```bash
curl http://localhost:8000/import/8f1e5efa-9c7d-4873-b7c4-3a8e65b3e724/logs
```

## Troubleshooting

If you encounter issues, check the following:

1. Verify the database and SFTP credentials in your `.env` file
2. Make sure the tables exist in the database
3. Check that column names match between the CSV and database
4. Look at the import logs for detailed error messages

## Security Notes

- Sensitive information is stored in environment variables, not in the code
- No credentials are logged or included in responses
- Used proper password handling and connection pooling
- SSL mode is enabled for PostgreSQL connections
