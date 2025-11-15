import time
import random
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed

PROJECT_ID = "bigdataproject-474006"
DATASET = "Glamira"
TABLE = "products"
MAX_RETRIES = 3
DELAY_RANGE = (0.2, 0.5)
LIMIT = 500 
MAX_WORKERS = 10 
bq_client = bigquery.Client.from_service_account_json(
    r"C:\Users\ToanMihn\Documents\DE_cource\BigDataProject\key.json"
)


def fetch_product_name(product_id, url: str) -> dict:
    """Crawl tên sản phẩm từ URL."""
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(*DELAY_RANGE))
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                el = soup.select_one("h1.page-title > span.base")
                name = el.text.strip() if el else ""
                return {"product_id": int(product_id), "product_name": name, "is_crawl": True}
            elif response.status_code in (403, 404):
                return {"product_id": int(product_id), "product_name": "", "is_crawl": True}
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"Lỗi {url}: {e}")
        time.sleep(1)
    return {"product_id": int(product_id), "product_name": "", "is_crawl": True}


def get_uncrawled_products(limit=LIMIT):
    """Đọc danh sách sản phẩm chưa crawl từ BigQuery."""
    query = f"""
    SELECT product_id, current_url
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE (is_crawl IS FALSE OR is_crawl IS NULL)
      AND current_url IS NOT NULL
    LIMIT {limit}
    """
    df = bq_client.query(query).to_dataframe()
    return df


def update_products(crawled_rows):
    """Cập nhật kết quả crawl vào BigQuery."""
    if not crawled_rows:
        print("Không có dữ liệu để cập nhật")
        return

    temp_table = f"{DATASET}.products_crawled_temp"
    temp_table_id = f"{PROJECT_ID}.{temp_table}"

    job = bq_client.load_table_from_json(
        crawled_rows,
        temp_table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("product_id", "INT64"),
                bigquery.SchemaField("product_name", "STRING"),
                bigquery.SchemaField("is_crawl", "BOOL"),
            ],
        ),
    )
    job.result()
    print(f"Ghi {len(crawled_rows)} dòng vào bảng tạm {temp_table}")

    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{TABLE}` T
    USING `{PROJECT_ID}.{temp_table}` S
    ON T.product_id = S.product_id
    WHEN MATCHED THEN
      UPDATE SET
        T.product_name = S.product_name,
        T.is_crawl = TRUE
    """
    bq_client.query(merge_query).result()
    print(f"Cập nhật bảng {TABLE} thành công")


def main_loop():
    while True:
        df = get_uncrawled_products(LIMIT)
        if df.empty:
            print("Không còn sản phẩm nào cần crawl, kết thúc.")
            break

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_product_name, row["product_id"], row["current_url"]): row
                for _, row in df.iterrows()
            }

            for future in as_completed(futures):
                res = future.result()
                pid = res["product_id"]
                name = res["product_name"]
                if name:
                    print(f"{pid} → {name}")
                else:
                    print(f"{pid} (rỗng hoặc lỗi)")
                results.append(res)

        update_products(results)
        time.sleep(1)

if __name__ == "__main__":
    main_loop()

