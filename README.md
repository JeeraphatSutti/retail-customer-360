# Retail Customer 360 & Feature Store Hybrid Cloud Pipeline for AI/ML

Production-grade Hybrid Data Platform ออกแบบและพัฒนาเพื่อประมวลผลข้อมูล Olist Brazilian E-Commerce สู่ Analytics Dashboard และ Feature Store สำหรับงาน AI/ML โดยผสาน Local Containerized Compute เข้ากับ Cloud Data Warehouse บน Google Cloud Platform (GCP)

---

## 1. System Architecture & Data Flow

ระบบใช้แนวคิด **Medallion Architecture** ในการจัดการและแบ่งระดับชั้นของข้อมูล:

- **Bronze Layer (Data Lake):** Raw CSV Data ingestion ขึ้นสู่ Google Cloud Storage (GCS)
- **Silver Layer (Data Warehouse):** ทำ Data Cleaning, Deduplication และ Data Normalization ผ่าน Apache Spark และ BigQuery
- **Gold Layer (Feature Store & Customer 360):** Aggregated Table วิเคราะห์ RFM Segmentation, พฤติกรรมการชำระเงิน และ Sentiment สำหรับพร้อมใช้งานบน BI และ AI/ML

## Architecture
![System Architecture](docs/images/architecture.png)

---

## 2. Tech Stack

Orchestration: Apache Airflow 2.x (Docker Compose, Local Executor, Postgres Backend)

Data Processing: Apache Spark (Standalone Master-Worker) + PySpark, dbt-core (dbt-bigquery)

Cloud Infrastructure: Google Cloud Platform (GCS, BigQuery, Service Account IAM)

Data Visualization: Google Looker Studio

---

## 3. Pipeline Execution & Lineage
3.1 Airflow PipelinesBronze Ingestion DAG: ดึงไฟล์ CSV จากเครื่อง Local โหลดขึ้น Google Cloud Storage (Bronze Layer)Silver-Gold Transformation DAG: เรียกใช้งาน Spark และ dbt ในการ Transform ข้อมูล Clean ข้อมูล และสร้าง Customer 360 Feature Store บน BigQuery

| Airflow Grid: Ingestion | Airflow Grid: Transformation |
| :---: | :---: |
| ![Airflow Ingestion](docs/images/olist_ingest.png) | ![Airflow Transformation](docs/images/olist_gold_dbt.png) |

---

3.2 BigQuery Storage & Gold Tables
Dataset / Target: gold.customer_360
![BigQuery Storage & Gold Tables](docs/images/bigquery.png)

ตาราง Gold รองรับทั้ง Demographic, RFM Segmentation, ช่องทางการชำระเงิน และ Average Review Score

---

## 4. Analytics Dashboard (Looker Studio)
แดชบอร์ดติดตามพฤติกรรมลูกค้าแบบ Customer 360 เชื่อมตรงกับตาราง gold.customer_360 บน BigQuery

![Analytics Dashboard](docs/images/dashboard.png)
https://datastudio.google.com/s/gsut9oUEX38

Key Metrics Summary:
Total Customers: 94,990 Unique Customers
Total Revenue: R$ 15,739,137.01
Avg Review Score: 4.11 / 5.0
Market Concentration: รัฐ SP (São Paulo) มีรายได้สูงสุดของแพลตฟอร์ม (> 5.8M BRL)

---

## 5. How to Run Locally
    1. Prerequisites & GCP Setup
        1.1 สร้าง Service Account บน GCP พร้อมสิทธิ์:
            Storage Object Admin
            BigQuery Admin

        1.2 วางคีย์ไฟล์ไว้ที่ credentials/gcp-key.json

        1.3 ตั้งค่า GCP Budget Alert สำหรับควบคุมค่าใช้จ่าย

    2. Environment Setup
    สร้างไฟล์ .env จาก .env.example:
    cp .env.example .env

    ระบุค่าคอนฟิก:
    GCP_PROJECT_ID=your-project-id
    GCS_BUCKET_NAME=your-bucket-name
    GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/gcp-key.json
    AIRFLOW_UID=50000

    3. Start Services
    docker compose up -d --build

    เข้าใช้งาน Airflow Webserver ได้ที่ http://localhost:8080 (Default: airflow / airflow)

---