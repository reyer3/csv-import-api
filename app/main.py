from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import uuid

from app.import_script import run_import

app = FastAPI(
    title="CSV Import API",
    description="API to trigger and monitor CSV imports from SFTP to PostgreSQL",
    version="1.0.0"
)

# Store for active and completed imports
import_tasks = {}

class ImportResponse(BaseModel):
    """Response with import job information"""
    job_id: str
    status: str
    start_time: str
    end_time: Optional[str] = None
    duration_seconds: Optional[float] = None
    downloaded_files: List[str] = []
    processed_files: List[tuple] = []
    errors: List[str] = []
    row_counts: Optional[Dict[str, int]] = None
    log_lines: Optional[int] = None

@app.get("/")
async def root():
    return {"message": "Data Import API is running"}

@app.post("/import", response_model=ImportResponse)
async def start_import(background_tasks: BackgroundTasks):
    """Start a new import job"""
    job_id = str(uuid.uuid4())
    
    # Create placeholder for job
    import_tasks[job_id] = {
        "status": "starting",
        "start_time": datetime.now().isoformat(),
        "result": None
    }
    
    # Run import in background
    background_tasks.add_task(run_import_job, job_id)
    
    return {
        "job_id": job_id,
        "status": "starting",
        "start_time": import_tasks[job_id]["start_time"],
        "log_lines": 0
    }

@app.get("/import/{job_id}", response_model=ImportResponse)
async def get_import_status(job_id: str):
    """Get status of an import job"""
    if job_id not in import_tasks:
        raise HTTPException(status_code=404, detail="Import job not found")
    
    job = import_tasks[job_id]
    
    # If job is completed, return full result
    if job["result"] is not None:
        result_dict = job["result"].to_dict()
        return {
            "job_id": job_id,
            "status": result_dict["status"],
            "start_time": result_dict["start_time"],
            "end_time": result_dict["end_time"],
            "duration_seconds": result_dict["duration_seconds"],
            "downloaded_files": result_dict["downloaded_files"],
            "processed_files": result_dict["processed_files"],
            "errors": result_dict["errors"],
            "row_counts": result_dict["row_counts"],
            "log_lines": len(result_dict["log_messages"])
        }
    
    # Otherwise return basic info
    return {
        "job_id": job_id,
        "status": job["status"],
        "start_time": job["start_time"],
        "log_lines": 0
    }

@app.get("/import/{job_id}/logs")
async def get_import_logs(job_id: str):
    """Get logs for an import job"""
    if job_id not in import_tasks:
        raise HTTPException(status_code=404, detail="Import job not found")
    
    job = import_tasks[job_id]
    
    if job["result"] is not None:
        return {"logs": job["result"].log_messages}
    
    return {"logs": ["Job still starting, no logs available"]}

async def run_import_job(job_id: str):
    """Execute the import job and update its status"""
    try:
        import_tasks[job_id]["status"] = "running"
        result = await run_import()
        import_tasks[job_id]["result"] = result
        import_tasks[job_id]["status"] = result.status
    except Exception as e:
        # Handle any unexpected errors
        import_tasks[job_id]["status"] = "failed"
        if import_tasks[job_id]["result"] is None:
            # If no result object exists, create one
            import_tasks[job_id]["result"] = {
                "status": "failed",
                "start_time": import_tasks[job_id]["start_time"],
                "end_time": datetime.now().isoformat(),
                "errors": [f"Unexpected error: {str(e)}"],
                "log_messages": [f"ERROR: Unexpected error: {str(e)}"]
            }
