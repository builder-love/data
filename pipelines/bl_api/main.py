from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

app = FastAPI()

# Database connection details (from environment variables)
DB_HOST = os.getenv("DATABASE_HOST")
DB_USER = os.getenv("DATABASE_USER")
DB_PASSWORD = os.getenv("DATABASE_PASSWORD")
DB_NAME = os.getenv("DATABASE_NAME")

# Pydantic Models (Data Validation)
class top_100_projects_forks(BaseModel):  # Replace with your actual data structure
    project_title: str
    latest_data_timestamp: str
    forks: int

# Database Connection Function (Dependency)
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursor_factory=RealDictCursor,  # Return results as dictionaries
        )
        yield conn
    finally:
        if conn:
           conn.close()


# API Endpoints
@app.get("/projects/top-forks", response_model=List[top_100_projects_forks])
async def get_top_100_forks(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the top 100 projects by forks from the api schema in postgresdatabase.
    """
    try:
        with db.cursor() as cur:
            cur.execute("SELECT * FROM api.top_100_forks;")  # Use your view
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/projects/top-forks/{project_title}", response_model=top_100_projects_forks)
async def get_top_100_forks_project(project_title: str, db: psycopg2.extensions.connection = Depends(get_db_connection)):
     """Retrieves a single project by project_title."""
     try:
        with db.cursor() as cur:
             cur.execute("SELECT * FROM api.top_100_forks WHERE project_title = %s;", (project_title,))
             result = cur.fetchone()
             if result is None:
                  raise HTTPException(status_code=404, detail="Project not found")
             return result
     except HTTPException:  # Re-raise HTTPException
         raise
     except Exception as e:
          raise HTTPException(status_code=500, detail=str(e))

# Add more endpoints as needed (e.g., for filtering, pagination, etc.)
