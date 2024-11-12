from fastapi import FastAPI, HTTPException, File, UploadFile, Query
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialize FastAPI app
app = FastAPI()

# Database connection details
DB_HOST = "localhost"
DB_NAME = "final_operativos"
DB_USER = "user"
DB_PASSWORD = "password"
DB_PORT = "5433"

# Connect to the database
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    return conn

# Define the endpoint to fetch students
@app.get("/students", response_model=List[dict])
def get_students():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)  # Use RealDictCursor for dictionary format
    try:
        # Execute the query to fetch all rows from the table
        cursor.execute("SELECT * FROM student_lifestyle LIMIT 100")
        students = cursor.fetchall()

        # Check if data is present
        if not students:
            raise HTTPException(status_code=404, detail="No data found")

        return students
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# Define S3 client without hardcoding credentials
s3_client = boto3.client("s3")

# Define the endpoint for file upload to S3
@app.post("/upload-file/")
async def upload_file(
        file: UploadFile = File(...),
        file_name: Optional[str] = Query(None),
        bucket_name: str = "ngaso-staging",
):
    try:
        # Use provided file name or default to the uploaded file's original name
        file_key = file_name if file_name else file.filename

        # Upload file to S3
        s3_client.upload_fileobj(
            file.file,
            bucket_name,
            file_key,
        )

        # Return the file URL
        file_url = f"https://{bucket_name}.s3.amazonaws.com/{file_key}"
        return {"file_url": file_url}

    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="AWS credentials not found.")
    except PartialCredentialsError:
        raise HTTPException(status_code=403, detail="Incomplete AWS credentials.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
