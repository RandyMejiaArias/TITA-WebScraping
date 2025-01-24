import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error

from utils.database import connection, cursor


def update_real_prices():
  """
  Actualiza los precios reales en la tabla `predictions` 
  usando los datos de la tabla `scraping-data`.
  """
  # Query para obtener predicciones sin precios reales
  query_select_predictions = """
  SELECT id, product_id, timestamp 
  FROM predictions 
  WHERE real_price IS NULL;
  """
  
  # Query para obtener precios reales desde `scraping-data`
  query_select_real_prices = """
  SELECT product_id, timestamp, price AS real_price
  FROM `scraping-data`
  WHERE (product_id) IN (
    SELECT product_id 
    FROM predictions 
    WHERE real_price IS NULL
  );
  """
  
  # Query para actualizar la columna `real_price`
  query_update = """
  UPDATE predictions
  SET real_price = %s
  WHERE id = %s;
  """
  
  try:
    # Leer predicciones sin precios reales
    cursor.execute(query_select_predictions)
    predictions_to_update = cursor.fetchall()
    
    if not predictions_to_update:
      print("No hay predicciones pendientes de actualización.")
      return
    
    print(f"Actualizando {len(predictions_to_update)} predicciones.")
    
    # Leer precios reales desde `scraping-data`
    cursor.execute(query_select_real_prices)
    real_prices = cursor.fetchall()

    print(f"{len(real_prices)} Precios reales leídos.")
    
    # Mapear los precios reales por `product_id` y `timestamp`
    real_prices_map = {
      (record['product_id'], record['timestamp'].split(" ")[0]): record['real_price']
      for record in real_prices
    }
    
    # Actualizar los precios reales para las predicciones correspondientes
    for prediction in predictions_to_update:
      key = (prediction['product_id'], prediction['timestamp'].strftime('%Y-%m-%d'))
      if key in real_prices_map:
        real_price = real_prices_map[key]
        print(f"Actualizando id={prediction['id']} con real_price={real_price}")
        cursor.execute(query_update, (real_price, prediction['id']))
    
    connection.commit()
    print("Precios reales actualizados correctamente.")
  except Exception as e:
    print(f"Error actualizando precios reales: {e}")

# Función para calcular el error con los precios reales
def calculate_updated_errors():
  """
  Calcula el error comparando las predicciones con los precios reales.
  Actualiza la tabla `model_errors` con los nuevos valores de MAE y RMSE.
  """
  query_predictions = """
  SELECT p.product_id, p.timestamp, p.predicted_price, p.real_price
  FROM predictions p
  WHERE p.real_price IS NOT NULL;
  """
  
  try:
    cursor.execute(query_predictions)
    results = cursor.fetchall()

    if not results:
      print("No hay datos para calcular errores.")
      return

    print("Calculando errores actualizados...")
    
    # Convertir resultados a DataFrame
    df = pd.DataFrame(results)
    
    # Calcular errores por producto
    updated_errors = []
    for product_id, group in df.groupby('product_id'):
      mae = mean_absolute_error(group['real_price'], group['predicted_price'])
      rmse = np.sqrt(mean_squared_error(group['real_price'], group['predicted_price']))
      updated_errors.append({'product_id': product_id, 'mae': mae, 'rmse': rmse})
    
    # Actualizar tabla `model_errors`
    update_query = """
    UPDATE model_errors
    SET mae = %s, rmse = %s
    WHERE product_id = %s;
    """
    for error in updated_errors:
      cursor.execute(update_query, (error['mae'], error['rmse'], error['product_id']))
    connection.commit()
    print("Errores actualizados correctamente.")
  except Exception as e:
    print(f"Error calculando errores actualizados: {e}")
  # finally:
  #   connection.close()

def fix_null_prices():
  query = """
  SELECT * 
  FROM predictions 
  WHERE timestamp <= NOW()
  """

  try:
    cursor.execute(query)
    results = cursor.fetchall()

    df = pd.DataFrame(results)
  except Exception as e:
    print(f"Error: {e}")

  # Procesamiento de los datos
  # Eliminar duplicados en 'product_id' y 'timestamp'
  df = df.drop_duplicates(subset=["product_id", "timestamp"])

  # Ordenar por 'product_id' y 'timestamp'
  df = df.sort_values(by=["product_id", "timestamp"]).reset_index(drop=True)

  # Interpolar valores de 'real_price' por cada 'product_id'
  df["real_price"] = (
      df.groupby("product_id")["real_price"]
      .apply(lambda group: group.interpolate(method="linear"))
      .reset_index(drop=True)
  )

  # Llenar valores restantes con 'predicted_price'
  df["real_price"] = df["real_price"].fillna(df["predicted_price"])

  # Actualizar la tabla 'predictions' con los valores corregidos
  update_query = """
  UPDATE predictions
  SET real_price = %s
  WHERE id = %s;
  """
  try:
    for index, row in df.iterrows():
      cursor.execute(update_query, (row["real_price"], row["id"]))
    connection.commit()
    print("Precios reales actualizados correctamente.")
  except Exception as e:
    print(f"Error actualizando precios reales: {e}")
