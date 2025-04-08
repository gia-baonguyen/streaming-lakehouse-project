# Dự án Giám sát E-commerce Thời gian thực với Kafka, Spark, Iceberg, Nessie, MinIO, Dremio & Streamlit

Dự án này trình bày cách xây dựng một pipeline xử lý dữ liệu và dashboard giám sát gần thời gian thực cho hoạt động thương mại điện tử (E-commerce). Hệ thống thu thập các sự kiện (đơn hàng, lượt click, lỗi), xử lý chúng bằng Spark Structured Streaming, lưu trữ dữ liệu trong các bảng Apache Iceberg được quản lý bởi Nessie trên MinIO, truy vấn dữ liệu thông qua Dremio và hiển thị kết quả trên một dashboard tương tác bằng Streamlit.

**Mục tiêu chính:**

* Theo dõi các chỉ số hiệu suất kinh doanh (KPIs) quan trọng như số lượng đơn hàng, doanh thu, người dùng hoạt động, tỷ lệ lỗi gần như ngay lập tức.
* Phát hiện sớm các dấu hiệu bất thường cơ bản (ví dụ: lỗi hệ thống nghiêm trọng).
* Cung cấp khả năng xem nhanh dữ liệu chi tiết (đơn hàng, lỗi mới nhất).

## Kiến trúc Tổng thể

Luồng dữ liệu chính của hệ thống như sau:

1.  **Ingestion:** Các ứng dụng nguồn (giả lập bằng script Python) gửi sự kiện vào các topic trên **Kafka**.
2.  **Processing:** **Spark Structured Streaming** đọc dữ liệu từ Kafka, thực hiện biến đổi, tính toán tổng hợp theo cửa sổ thời gian và phát hiện cảnh báo đơn giản.
3.  **Storage & Catalog:** Dữ liệu đã xử lý được ghi vào các bảng **Apache Iceberg**. Metadata của bảng (schema, snapshots, vị trí file) được quản lý bởi **Nessie Catalog**, trong khi các file dữ liệu thực tế (Parquet) được lưu trữ trên **MinIO** (S3-compatible).
4.  **Querying:** **Dremio** kết nối với Nessie để truy vấn dữ liệu trong các bảng Iceberg bằng SQL.
5.  **Visualization:** **Streamlit** kết nối với Dremio qua **Arrow Flight** để lấy dữ liệu và hiển thị dashboard tương tác cho người dùng.

## Công nghệ sử dụng

* **Apache Kafka:** Hệ thống message queue phân tán, làm trung tâm thu thập sự kiện. (Sử dụng chế độ KRaft).
* **Apache Spark (Structured Streaming):** Framework xử lý dữ liệu phân tán và streaming.
* **MinIO:** Lưu trữ object tương thích S3, làm backend cho Data Lake.
* **Nessie:** Máy chủ Iceberg Catalog, quản lý metadata và cung cấp giao dịch ACID, versioning dữ liệu.
* **Apache Iceberg:** Định dạng bảng mã nguồn mở cho dữ liệu lớn trên Data Lake.
* **Dremio:** Công cụ truy vấn dữ liệu (SQL Query Engine) hiệu năng cao cho Data Lakehouse.
* **Streamlit:** Framework Python để xây dựng ứng dụng web dữ liệu tương tác.
* **Apache Arrow Flight:** Giao thức truyền dữ liệu hiệu năng cao giữa Dremio và Streamlit.
* **Docker & Docker Compose:** Để đóng gói và chạy các thành phần hạ tầng.
* **Python:** Ngôn ngữ lập trình chính cho Spark job, producers và dashboard Streamlit.

## Các Chức năng Chính

* Thu thập dữ liệu đơn hàng, lượt click, lỗi hệ thống theo thời gian thực.
* Xử lý stream dữ liệu với Spark để làm sạch, biến đổi và tính toán các chỉ số tổng hợp (KPIs) theo phút, theo cửa sổ thời gian.
* Lưu trữ dữ liệu có cấu trúc và dữ liệu tổng hợp vào các bảng Iceberg trên MinIO, với metadata được quản lý bởi Nessie.
* Phát hiện và ghi lại các cảnh báo đơn giản (ví dụ: lỗi FATAL).
* Truy vấn dữ liệu gần thời gian thực bằng SQL thông qua Dremio.
* Hiển thị dashboard tương tác với các KPIs, biểu đồ xu hướng, danh sách cảnh báo và log hoạt động mới nhất bằng Streamlit.

## Cấu trúc Dự án
```
.
├── docker-compose.yml          # Defines infrastructure services (Kafka, Spark, Nessie, MinIO, Dremio)
├── spark-apps/                 # Spark processing code
│   └── streaming_processor.py  # PySpark stream processing script
├── producers/                  # Data source simulation code
│   ├── producer_orders.py
│   ├── producer_clicks.py
│   ├── producer_errors.py
│   └── requirements.txt        # Python requirements for producers
├── dashboard/                  # Dashboard application code
│   ├── dashboard.py
│   └── requirements.txt        # Python requirements for dashboard
├── .gitignore                  # Files/directories ignored by Git
├── images/                     # Directory for README images
│   └── architecture.png        # Example diagram image name
├── nessie-data/                # (Auto-generated, ignored by .gitignore)
├── minio-data/                 # (Auto-generated, ignored by .gitignore)
└── README.md                   # Guide 
```
## Hướng dẫn Cài đặt và Chạy

**Yêu cầu:**

* Docker
* Docker Compose
* Python 3.x (khuyến nghị sử dụng môi trường ảo - venv)
* `pip` (Python package installer)

**Các bước thực hiện:**

1.  **Clone Repository:**
    ```bash
    git clone <URL_repository_cua_ban>
    cd <ten_thu_muc_repository>
    ```

2.  **Khởi chạy Hạ tầng Docker:**
    * Mở terminal trong thư mục gốc của dự án.
    * Chạy lệnh: `docker-compose up -d`
    * Đợi vài phút để tất cả các service khởi động hoàn toàn. Bạn có thể kiểm tra trạng thái bằng `docker-compose ps`. Đảm bảo MinIO đã chạy xong entrypoint script để tạo bucket.

3.  **Cấu hình Dremio:**
    * Truy cập Dremio Web UI: `http://localhost:9047`
    * Lần đầu tiên, tạo tài khoản quản trị (ví dụ: user `dremio`, password `dremio123` - **nên đổi mật khẩu khác**).
    * Trong giao diện Dremio, đi đến mục **Sources** (góc dưới bên trái).
    * Nhấn nút **Add Source** (dấu cộng) -> Chọn **Nessie**.
    * Điền các thông tin sau:
        * **Name:** Đặt tên cho source (ví dụ: `nessie_minio_catalog` - **quan trọng:** phải khớp với biến `ICEBERG_CATALOG_NAME` trong file Python và Dremio source name trong `dashboard.py`).
        * **Endpoint URL:** `http://nessie:19120/api/v2`
        * **Authentication:** Chọn "None".
    * Chuyển qua tab **Storage**:
        * **AWS root path:** `warehouse` 
        * **AWS Access Key:** `admin` (user của MinIO trong docker-compose)
        * **AWS Access Secret:** `password` (password của MinIO trong docker-compose)
    * Nhấn vào **Advanced Options** (vẫn trong tab Storage):
        * Check vào ô **"Enable compatibility mode (allow S3-compatible storage)"**.
        * Trong phần **Connection Properties**, nhấn **Add Property** và thêm 2 thuộc tính:
            * `fs.s3a.endpoint` : `minio:9000`
            * `fs.s3a.path.style.access` : `true`
            * `dremio.s3.compat` : `true`
        * Bỏ check ô "Encrypt connection".
    * Nhấn **Save**. Dremio sẽ kết nối đến Nessie và MinIO. Bạn nên thấy database `ecommerce_db` (sẽ được Spark tạo sau) xuất hiện dưới source này sau khi Spark job chạy.

4.  **Chạy Kafka Producers:**
    * Mở **3 terminal riêng biệt** trên máy host của bạn, trong thư mục gốc của dự án.
    * **(Khuyến nghị)** Tạo và kích hoạt môi trường ảo Python:
        ```bash
        # Terminal 1, 2, 3:
        python -m venv venv
        # Windows:
        .\venv\Scripts\activate
        # MacOS/Linux:
        source venv/bin/activate
        ```
    * Cài đặt thư viện Kafka cho Python (chỉ cần làm 1 lần cho môi trường ảo):
        ```bash
        # Terminal 1, 2, 3 (sau khi kích hoạt venv):
        pip install -r producers/requirements.txt
        ```
    * Chạy các producer:
        ```bash
        # Terminal 1:
        python producers/producer_orders.py

        # Terminal 2:
        python producers/producer_clicks.py

        # Terminal 3:
        python producers/producer_errors.py
        ```
    * Để các terminal này chạy để liên tục sinh dữ liệu.

5.  **Chạy Spark Job:**
    * Truy cập giao diện JupyterLab của container Spark: `http://localhost:8888`
    * Sử dụng giao diện JupyterLab để upload file `spark-apps/streaming_processor.py` vào thư mục `/workspace/`.
    * Mở một cửa sổ Terminal mới trong JupyterLab (File -> New -> Terminal).
    * **Quan trọng:** Đặt các biến môi trường cần thiết cho lệnh `spark-submit`:
        ```bash
        export SPARK_VERSION=3.5.0 
        export ICEBERG_VERSION=1.4.2
        export NESSIE_VERSION=0.75.0
        ```
    * Chạy lệnh `spark-submit` (đảm bảo không có lỗi cú pháp, ký tự thừa):
        ```bash
        /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},\
        org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},\
        org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},\
        org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:${NESSIE_VERSION} \
        --conf spark.sql.catalog.nessie_minio_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.nessie_minio_catalog.s3.endpoint=http://minio:9000 \
        --conf spark.sql.catalog.nessie_minio_catalog.s3.path-style-access=true \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        /workspace/streaming_processor.py
        ```
    * Theo dõi output trong terminal JupyterLab. Bạn sẽ thấy các thông báo khởi tạo Spark Session, tạo Namespace, tạo Bảng, và sau đó là "All streaming jobs started. Awaiting termination...".

6.  **Chạy Streamlit Dashboard:**
    * Mở một terminal **mới** trên máy host, trong thư mục gốc của dự án.
    * Kích hoạt lại môi trường ảo (nếu bạn đã đóng terminal trước đó):
        ```bash
        # Windows:
        .\venv\Scripts\activate
        # MacOS/Linux:
        source venv/bin/activate
        ```
    * Cài đặt các thư viện cần thiết cho Streamlit:
        ```bash
        pip install -r dashboard/requirements.txt
        ```
    * **Kiểm tra lại file `dashboard.py`:** Đảm bảo các biến `DREMIO_USER`, `DREMIO_PASSWORD`, và `DREMIO_NESSIE_SOURCE` ở đầu file khớp với thông tin đăng nhập Dremio bạn đã tạo và tên Source bạn đã đặt trong Dremio UI.
    * Chạy ứng dụng Streamlit:
        ```bash
        streamlit run dashboard/dashboard.py
        ```
    * Mở trình duyệt và truy cập `http://localhost:8501`. Bạn sẽ thấy dashboard hiển thị dữ liệu được cập nhật gần thời gian thực.

## Gỡ lỗi (Troubleshooting)

* **Lỗi Kafka Producer (`KafkaTimeoutError`, `NoBrokersAvailable`):** Kiểm tra lại `KAFKA_ADVERTISED_LISTENERS` trong `docker-compose.yml` đã được sửa thành `PLAINTEXT://localhost:xxxxx` tương ứng với cổng map ra host chưa. Đảm bảo Kafka brokers đã khởi động xong.
* **Lỗi Spark (`unresolved dependency`, `NoSuchMethodError`, `ClassNotFoundException`):** Thường liên quan đến xung đột thư viện. Đảm bảo bạn sử dụng đúng các gói `--packages` như trong lệnh `spark-submit` cuối cùng (đặc biệt là `iceberg-aws-bundle`). Xóa cache Ivy (`/root/.ivy2/cache` trong container Spark) có thể hữu ích trong một số trường hợp (`docker exec spark rm -rf /root/.ivy2/cache`).
* **Lỗi Spark (`NoSuchTableException`):** Đảm bảo script `streaming_processor.py` có các lệnh `CREATE TABLE IF NOT EXISTS` cho tất cả các bảng đích *trước khi* gọi `writeStream`.
* **Lỗi Spark (`S3Exception: Status Code 301`):** Đảm bảo các cấu hình `--conf` sau được đặt đúng trong lệnh `spark-submit`:
    * `--conf spark.sql.catalog.nessie_minio_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO`
    * `--conf spark.sql.catalog.nessie_minio_catalog.s3.endpoint=http://minio:9000`
    * `--conf spark.sql.catalog.nessie_minio_catalog.s3.path-style-access=true`
    * `--conf spark.hadoop.fs.s3a.path.style.access=true`
    (Thay `nessie_minio_catalog` bằng tên catalog bạn đặt nếu khác).
* **Lỗi Streamlit (`Column 'end' not found`):** Đảm bảo các câu truy vấn SQL trong `dashboard.py` sử dụng tên cột `window_start` và `window_end` thay vì `start` và `end` cho các bảng metrics.
* **Lỗi Streamlit (`TO_TIMESTAMP does not support operand types (TIMESTAMP)`):** Xóa các hàm `TO_TIMESTAMP()` không cần thiết trong các câu truy vấn SQL của `dashboard.py` khi cột đó đã là kiểu TIMESTAMP.
* **Lỗi Streamlit (`ArrowIOError`, `Connection refused`):** Kiểm tra Dremio service có đang chạy không (`docker-compose ps`). Kiểm tra `DREMIO_FLIGHT_HOST`, `DREMIO_FLIGHT_PORT`, `DREMIO_USER`, `DREMIO_PASSWORD` trong `dashboard.py`.

## Cải tiến tiềm năng

* Triển khai logic phát hiện bất thường phức tạp hơn trong Spark (ví dụ: so sánh với trung bình động, độ lệch chuẩn).
* Sử dụng Kafka Connect và Debezium để lấy dữ liệu CDC từ database thật thay vì producer giả lập.
* Tối ưu hóa hiệu năng Spark Streaming (partitioning Kafka topic, cấu hình Spark executor/memory).
* Thêm nhiều chỉ số và biểu đồ vào dashboard Streamlit.
* Triển khai hệ thống trên môi trường cloud hoặc Kubernetes thay vì Docker Compose local.