# 🧩 Library Update Notes

## 📅 Cập nhật ngày: 2025-10-08

---

### 1️⃣ Bổ sung các hàm thông dụng cho Hive
**Mô tả:**  
- Thêm nhóm hàm tiện ích hỗ trợ thao tác nhanh với HiveQL.  
- Bao gồm:
  - `get_table_schema()`: Lấy thông tin schema của bảng Hive.
  - `drop_partition_if_exists()`: Kiểm tra và xoá partition nếu tồn tại.
  - `repair_table()`: Chạy lệnh `MSCK REPAIR TABLE` để đồng bộ metadata.
  - `load_data_to_table()`: Hỗ trợ `LOAD DATA INPATH ... INTO TABLE`.
  - `run_hql()`: Hàm helper để thực thi HiveQL an toàn với logging.

**Mục đích:**  
→ Chuẩn hóa thao tác Hive trong ETL framework, tránh lặp code giữa các pipeline.

---

### 2️⃣ Bổ sung thêm engine cho hệ thống Alert  
**Mô tả:**  
- Tạo thêm các engine cảnh báo để hỗ trợ nhiều kênh thông báo:
  - `TelegramAlertEngine`: Gửi cảnh báo qua Telegram bot.
  - `PostgresAlertEngine`: Ghi log cảnh báo vào bảng PostgreSQL (phục vụ dashboard hoặc audit).
- Hoàn thiện file `MakeAlertEngine.py`:
  - Chuẩn hóa interface `BaseAlertEngine`.
  - Tự động load cấu hình (token, URL, DB config) từ `.env`.
  - Cho phép đăng ký nhiều engine song song.

**Ví dụ:**
```python
engine = MakeAlertEngine()
engine.alert("Pipeline failed!", level="ERROR")
```

---

### 3️⃣ Viết decorator gửi alert khi có lỗi
**Mô tả:**  
- Thêm decorator `@alert_on_error` dùng để tự động gửi thông báo lỗi đến các alert engine khi hàm gặp exception.  
- Cho phép tùy chọn message, severity level, và engine cụ thể.

**Ví dụ sử dụng:**
```python
from alert_decorator import alert_on_error

@alert_on_error(level="CRITICAL")
def run_etl_job():
    raise RuntimeError("ETL job failed due to missing partition!")
```

**Kết quả:**  
→ Tự động gửi cảnh báo qua các kênh đã cấu hình mà không cần try/except thủ công.

---

### 4️⃣ Hoàn thiện MakeDbConnector.py
**Mô tả:**  
- Tích hợp khả năng **load cấu hình từ `.env`** (qua `dotenv` hoặc `os.getenv`).
- Hỗ trợ nhiều loại kết nối: PostgreSQL, MySQL, ClickHouse, Hive, SQLite.
- Chuẩn hóa cấu trúc `DbConnector` để dùng chung trong các service.

**Ví dụ:**
```python
from db_connector import MakeDbConnector

db = MakeDbConnector("POSTGRES")
conn = db.connect()
```

**Cấu hình mẫu `.env`:**
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=secret
POSTGRES_DB=analytics
```

---

### 5️⃣ [Dự phòng cập nhật tiếp theo]
*(Ví dụ: Hoàn thiện logging engine, bổ sung test unit, cải thiện performance, v.v.)*

---

## 🧠 Ghi chú thêm
- Tất cả các module mới cần được đăng ký trong `__init__.py` của thư viện.
- Nên bổ sung test cho từng engine và decorator (dùng `pytest`).
- Khi release, cập nhật version trong `setup.py` hoặc `pyproject.toml`.

---

**Người thực hiện:** _[Tên bạn]_  
**Reviewer:** _[Tên reviewer]_  
**Phiên bản:** `vX.Y.Z`
