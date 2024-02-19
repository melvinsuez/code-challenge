from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess


def task4():

    querry = """
    SELECT
    jsonb_object_agg(
        orders.order_id,
        jsonb_build_object(
            'order_id', orders.order_id,
            'order_date', orders.order_date,
            'shipped_date', orders.shipped_date,
            'ship_name', orders.ship_name,
            'ship_address', orders.ship_address,
            'ship_city', orders.ship_city,
            'ship_postal_code', orders.ship_postal_code,
            'ship_country', orders.ship_country,
            'customer', jsonb_build_object(
                'company_name', customers.company_name,
                'contact_name', customers.contact_name,
                'phone', customers.phone
            ),
            'details', details_json.details
        )
    ) AS orders_json
FROM
    orders
INNER JOIN customers ON orders.customer_id = customers.customer_id
LEFT JOIN (
    SELECT
        order_id,
        jsonb_agg(
            jsonb_build_object(
                'product_id', order_details.product_id,
                'quantity', order_details.quantity,
                'unit_price', order_details.unit_price,
                'discount', order_details.discount,
                'product_name', products.product_name,
                'quantity_per_unit', products.quantity_per_unit
            )
        ) AS details
    FROM
        order_details
    INNER JOIN products ON order_details.product_id = products.product_id
    GROUP BY
        order_details.order_id
) AS details_json ON orders.order_id = details_json.order_id
GROUP BY
    orders.order_id
    """

    # Construir la cadena de conexión para psql
    connection_string = "-h postgres -U northwind_user -d postgres"

    # Ejecutar la consulta y guardar el resultado en un archivo JSON
    subprocess.run(f'psql {connection_string} -c "{querry}" > /opt/airflow/data/postgres/result01.json', shell=True)
