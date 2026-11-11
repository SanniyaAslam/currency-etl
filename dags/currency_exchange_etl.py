from __future__ import annotations

from datetime import datetime, timedelta

import requests
try:
    from airflow.sdk import dag, task  # Airflow 3
except ImportError:
    from airflow.decorators import dag, task  # Airflow 2
from airflow.providers.mongo.hooks.mongo import MongoHook

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE = "EUR"
QUOTES = "USD,GBP,JPY"
SOURCE = "Frankfurter API"

DB_NAME = "currency_db"
COLLECTION = "exchange_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency rates: Frankfurter API -> transform -> MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task
    def extract_exchange_rates(**context):
        """Task 1: call the API and push raw JSON to a named XCom."""
        resp = requests.get(API_URL, params={"base": BASE, "quotes": QUOTES}, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        context["ti"].xcom_push(key="raw_exchange_rates", value=raw)
        print(f"Extracted: {raw}")

    @task
    def transform_exchange_rates(**context):
        """Task 2: pull raw data (named XCom), reshape, push transformed docs."""
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key="raw_exchange_rates")
        retrieved_at = datetime.now().replace(microsecond=0).isoformat()

        # v2 returns a list of {date, base, quote, rate}; also handle the
        # older {"date":..., "base":..., "rates": {...}} shape just in case.
        records = []
        if isinstance(raw, list):
            for r in raw:
                records.append((r["base"], r["quote"], r["rate"], r["date"]))
        else:
            for quote, rate in raw["rates"].items():
                records.append((raw["base"], quote, rate, raw["date"]))

        transformed = [
            {
                "base_currency": base,
                "target_currency": quote,
                "exchange_rate": float(rate),
                "rate_date": rate_date,
                "source": SOURCE,
                "retrieved_at": retrieved_at,
            }
            for base, quote, rate, rate_date in records
        ]
        ti.xcom_push(key="transformed_exchange_rates", value=transformed)
        print(f"Transformed: {transformed}")

    @task
    def load_to_mongodb(**context):
        """Task 3: pull transformed docs (named XCom) and upsert into MongoDB."""
        docs = context["ti"].xcom_pull(
            task_ids="transform_exchange_rates", key="transformed_exchange_rates"
        )
        client = MongoHook(mongo_conn_id="mongo_default").get_conn()
        col = client[DB_NAME][COLLECTION]
        for d in docs:
            # upsert keeps reruns idempotent (one doc per pair per rate_date)
            col.update_one(
                {
                    "base_currency": d["base_currency"],
                    "target_currency": d["target_currency"],
                    "rate_date": d["rate_date"],
                },
                {"$set": d},
                upsert=True,
            )
        print(f"Loaded {len(docs)} documents into {DB_NAME}.{COLLECTION}")

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()
