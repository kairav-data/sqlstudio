import pyodbc
import io
import pandas as pd
import csv
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

app = FastAPI()

# expert ui requires cors enabled for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class DynamicRequest(BaseModel):
    SERVER: str
    DATABASE: str
    USERNAME: str
    PASSWORD: str
    QUERY: str = ""
    DELIMITER: str = ","
    FORMAT: str = "csv"

# helper to convert problematic sql types (geometry, xml) to strings
def handle_special_types(value):
    return str(value) if value is not None else ""

def get_conn_str(req: DynamicRequest):
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={req.SERVER};"
        f"DATABASE={req.DATABASE};"
        f"UID={req.USERNAME};"
        f"PWD={req.PASSWORD};"
        f"Connection Timeout=10;"
    )

def get_db_connection(req: DynamicRequest):
    try:
        conn = pyodbc.connect(get_conn_str(req))
        # register converters for xml (-152) and geometry (-151)
        conn.add_output_converter(-151, handle_special_types)
        conn.add_output_converter(-152, handle_special_types)
        return conn
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection Error: {str(e)}")

# --- endpoint 1: test connection ---
@app.post("/test-connection")
async def test_connection(req: DynamicRequest):
    conn = get_db_connection(req)
    conn.close()
    return {"status": "success", "message": "connected"}

# --- endpoint 2: execute for preview ---
@app.post("/execute-sql")
async def execute_sql(req: DynamicRequest):
    conn = get_db_connection(req)
    try:
        # 1. Get the data
        df = pd.read_sql(req.QUERY.lower(), conn)
        
        # 2. Convert NaNs to None (null)
        clean_list = df.replace({np.nan: None}).to_dict(orient="records")
        
        # 3. Explicitly wrap in a "data" key
        return {"data": clean_list}
        
    except Exception as e:
        print(f"Error: {e}") # This shows in your Python terminal
        raise HTTPException(status_code=500, detail=str(e))

# --- endpoint 3: download file ---
@app.post("/download")
async def download_file(req: DynamicRequest):
    conn = get_db_connection(req)
    try:
        df = pd.read_sql(req.QUERY, conn)
        conn.close()

        buffer = io.BytesIO()
        
        if req.FORMAT == "excel":
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='SQL_Results')
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ext = "xlsx"
        else:
            # handle csv/txt with custom delimiter
            text_data = df.to_csv(index=False, sep=req.DELIMITER)
            buffer.write(text_data.encode('utf-8'))
            media_type = "text/csv" if req.FORMAT == "csv" else "text/plain"
            ext = req.FORMAT

        buffer.seek(0)
        return StreamingResponse(
            buffer,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename=query_export.{ext}"}
        )
    except Exception as e:
        if conn: conn.close()
        raise HTTPException(status_code=400, detail=f"Export Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)