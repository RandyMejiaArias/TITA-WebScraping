import os
from dotenv import load_dotenv
import pymysql
from datetime import datetime, timedelta

load_dotenv()

# Configuración de MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

# Conexión a MySQL
config = {
  'user': MYSQL_USER,
  'password': MYSQL_PASSWORD,
  'host': MYSQL_HOST,
  'port': 3306,
  "database": "tita_pbi",
  'cursorclass': pymysql.cursors.DictCursor
}


def etl_update():
  try:
    # Conexión a la base de datos
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    # Fecha de la última actualización
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # Consulta para actualizar la tabla de hechos
    etl_query = f"""
    INSERT INTO fact_predictions (
      product_id,
      scraped_date,
      real_price,
      predicted_price,
      day,
      month,
      day_of_week,
      days_since_start,
      rating,
      moving_avg_3,
      moving_avg_7,
      mae,
      rmse
    )
    SELECT 
      p.product_id,
      p.timestamp as scraped_date,
      MAX(p.real_price) AS real_price,
      MAX(p.predicted_price) AS predicted_price,
      MAX(p.day) AS day,
      MAX(p.month) AS month,
      MAX(p.day_of_week) AS day_of_week,
      MAX(p.days_since_start) AS days_since_start,
      MAX(p.rating) AS rating,
      MAX(p.moving_avg_3) AS moving_avg_3,
      MAX(p.moving_avg_7) AS moving_avg_7,
      mae,
      rmse
    FROM 
      tita.predictions p
    LEFT JOIN 
      tita.`scraping-data` s  
    ON 
      p.product_id = s.product_id
      AND DATE(p.timestamp) = DATE(s.timestamp)
    LEFT JOIN 
      tita.model_errors e ON p.product_id = e.product_id
      and SUBSTRING(p.timestamp, 1, 10) = SUBSTRING(e.timestamp, 1, 10)
    WHERE 
      DATE(p.timestamp) BETWEEN '{yesterday}' AND '{today}'
    GROUP BY 
        p.timestamp, p.product_id;
    """

    # Ejecutar la consulta
    cursor.execute(etl_query)
    conn.commit()
    print(f"Tabla de hechos actualizada con éxito para el rango {yesterday} - {today}")

  except pymysql.MySQLError as err:
    print(f"Error: {err}")
  except Exception as e:
    print(f"Error en el proceso ETL: {e}")
  finally:
    cursor.close()
    conn.close()