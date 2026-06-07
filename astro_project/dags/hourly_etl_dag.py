from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import json


@dag(
    dag_id="hourly_json_etl",
    description="Hourly ETL DAG: extract JSON, flatten it, load into DataFrame",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "json", "pandas"],
)
def hourly_json_etl():

    @task
    def extract():
        """
        Generate sample nested JSON data.
        """
        data = [
            {
                "id": 1,
                "user": {
                    "name": "Artur",
                    "email": "artur@example.com",
                    "address": {
                        "city": "Chernivtsi",
                        "country": "Ukraine"
                    }
                },
                "order": {
                    "product": "Laptop",
                    "price": 45000,
                    "currency": "UAH"
                }
            },
            {
                "id": 2,
                "user": {
                    "name": "Ivan",
                    "email": "ivan@example.com",
                    "address": {
                        "city": "Kyiv",
                        "country": "Ukraine"
                    }
                },
                "order": {
                    "product": "Mouse",
                    "price": 900,
                    "currency": "UAH"
                }
            },
            {
                "id": 3,
                "user": {
                    "name": "Olena",
                    "email": "olena@example.com",
                    "address": {
                        "city": "Lviv",
                        "country": "Ukraine"
                    }
                },
                "order": {
                    "product": "Keyboard",
                    "price": 2500,
                    "currency": "UAH"
                }
            }
        ]

        print("Extracted JSON data:")
        print(json.dumps(data, indent=4, ensure_ascii=False))

        return data

    @task
    def transform(data):
        """
        Reduce JSON nesting by flattening nested fields.
        """
        transformed_data = []

        for item in data:
            flat_item = {
                "id": item["id"],
                "user_name": item["user"]["name"],
                "user_email": item["user"]["email"],
                "city": item["user"]["address"]["city"],
                "country": item["user"]["address"]["country"],
                "product": item["order"]["product"],
                "price": item["order"]["price"],
                "currency": item["order"]["currency"],
            }

            transformed_data.append(flat_item)

        print("Transformed flat JSON data:")
        print(json.dumps(transformed_data, indent=4, ensure_ascii=False))

        return transformed_data

    @task
    def load(data):
        """
        Create DataFrame and print it to console.
        """
        df = pd.DataFrame(data)

        print("Loaded DataFrame:")
        print(df)

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


hourly_json_etl()