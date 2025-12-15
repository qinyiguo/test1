from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
from datetime import datetime

app = FastAPI(title="Excel Import API")

# CORS 設置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 資料庫連接配置
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "sjc1.clusters.zeabur.com"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "Ig30Hlx5Uz7L8pyc9CbtK2EFG4XoM6i1"),
    "database": os.getenv("DB_NAME", "zeabur"),
}

def get_db_connection():
    """獲取資料庫連接"""
    return psycopg2.connect(**DB_CONFIG)

@app.get("/")
def read_root():
    return {"message": "Excel Import API is running"}

# ==================== 上傳 Excel 的 API ====================

@app.post("/upload/provincial-operations")
async def upload_provincial_operations(file: UploadFile = File(...)):
    """上傳全省營運數據"""
    return await upload_excel(file, "provincial_operations")

@app.post("/upload/parts-sales")
async def upload_parts_sales(file: UploadFile = File(...)):
    """上傳零件銷售資料"""
    return await upload_excel(file, "parts_sales")

@app.post("/upload/repair-income")
async def upload_repair_income(file: UploadFile = File(...)):
    """上傳維修收入明細"""
    return await upload_excel(file, "repair_income_details")

@app.post("/upload/technician-performance")
async def upload_technician_performance(file: UploadFile = File(...)):
    """上傳技師績效"""
    return await upload_excel(file, "technician_performance")

async def upload_excel(file: UploadFile, table_name: str):
    """通用 Excel 上傳函數"""
    try:
        # 讀取 Excel
        df = pd.read_excel(file.file)
        
        # 連接資料庫
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 逐行匯入
        inserted_count = 0
        for index, row in df.iterrows():
            # 將 NaN 轉換為 None
            data_dict = row.where(pd.notna(row), None).to_dict()
            
            cursor.execute(
                f"INSERT INTO {table_name} (file_name, row_number, data) VALUES (%s, %s, %s)",
                (file.filename, index + 1, json.dumps(data_dict, ensure_ascii=False, default=str))
            )
            inserted_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": f"成功匯入 {inserted_count} 筆數據",
            "table": table_name,
            "rows": inserted_count,
            "filename": file.filename
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "table": table_name
        }

# ==================== 查詢數據的 API ====================

@app.get("/data/{table_name}")
def get_data(table_name: str, limit: int = 100, offset: int = 0):
    """查詢表中的所有數據"""
    try:
        # 驗證表名（防止 SQL 注入）
        valid_tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        if table_name not in valid_tables:
            raise HTTPException(status_code=400, detail="Invalid table name")
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 查詢總數
        cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
        total = cursor.fetchone()["total"]
        
        # 查詢數據
        cursor.execute(
            f"SELECT id, file_name, row_number, data, created_at FROM {table_name} ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (limit, offset)
        )
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "table": table_name,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": rows
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/data/{table_name}/{id}")
def get_single_row(table_name: str, id: int):
    """查詢單筆數據"""
    try:
        valid_tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        if table_name not in valid_tables:
            raise HTTPException(status_code=400, detail="Invalid table name")
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            f"SELECT * FROM {table_name} WHERE id = %s",
            (id,)
        )
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Data not found")
        
        return {"status": "success", "data": row}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 修改數據的 API（管理者） ====================

@app.put("/data/{table_name}/{id}")
def update_data(table_name: str, id: int, updated_data: dict):
    """修改單筆數據（管理者功能）"""
    try:
        valid_tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        if table_name not in valid_tables:
            raise HTTPException(status_code=400, detail="Invalid table name")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 更新 data 欄位
        cursor.execute(
            f"UPDATE {table_name} SET data = %s, updated_at = %s WHERE id = %s",
            (json.dumps(updated_data, ensure_ascii=False, default=str), datetime.now(), id)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "數據已更新",
            "table": table_name,
            "id": id
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 統計數據 ====================

@app.get("/stats")
def get_stats():
    """獲取所有表的統計信息"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        stats = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()["count"]
            stats[table] = count
        
        cursor.close()
        conn.close()
        
        return {"status": "success", "stats": stats}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

