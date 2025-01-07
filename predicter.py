import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from database import output_collection, connection, cursor

def daily_prediction():
  # Cargar datos históricos desde la base de datos
  query = """
  SELECT * 
  FROM `scraping-data`
  WHERE timestamp >= (SELECT MIN(timestamp) FROM `scraping-data`)
  ORDER BY product_id, timestamp;
  """

  try:
    cursor.execute(query)
    results = cursor.fetchall()

    # Obtener nombres de las columnas de la consulta
    columns = [desc[0] for desc in cursor.description]
        
    # Crear DataFrame con los resultados y nombres de las columnas
    data = pd.DataFrame(results, columns=columns)
  except Exception as e:
    connection.close()
    print(f"Error: {e}")
    return

  # Convertir 'timestamp' a datetime y ordenar
  data['timestamp'] = pd.to_datetime(data['timestamp'])
  data = data.sort_values(by=['product_id', 'timestamp'])

  # Agregar características temporales
  data['day'] = data['timestamp'].dt.day
  data['month'] = data['timestamp'].dt.month
  data['day_of_week'] = data['timestamp'].dt.dayofweek
  data['days_since_start'] = (data['timestamp'] - data['timestamp'].min()).dt.days

  # Calcular promedios móviles
  data['moving_avg_3'] = data.groupby('product_id')['price'].transform(lambda x: x.rolling(window=3, min_periods=1).mean())
  data['moving_avg_7'] = data.groupby('product_id')['price'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())

  predictions_list = []
  errors_list = []

  # Iterar por cada product_id
  for product_id, group in data.groupby('product_id'):
    print(f"Procesando Product ID: {product_id}")
    X = group[['rating', 'day', 'month', 'day_of_week', 'days_since_start', 'moving_avg_3', 'moving_avg_7']]
    y = group['price']

    # Entrenar modelo
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)

    # Predicciones históricas
    group['predicted_price'] = model.predict(X)
    mae = mean_absolute_error(group['price'], group['predicted_price'])
    rmse = np.sqrt(mean_squared_error(group['price'], group['predicted_price']))
    errors_list.append({'product_id': product_id, 'mae': mae, 'rmse': rmse})

    # Predicciones futuras para los próximos 3 días
    last_date = group['timestamp'].max()
    future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, 4)]
    future_days_since_start = [(date - data['timestamp'].min()).days for date in future_dates]
    future_data = pd.DataFrame({
      'timestamp': future_dates,
      'day': [date.day for date in future_dates],
      'month': [date.month for date in future_dates],
      'day_of_week': [date.dayofweek for date in future_dates],
      'days_since_start': future_days_since_start,
      'rating': [group['rating'].mean()] * len(future_dates),
      'moving_avg_3': [group['price'].iloc[-3:].mean()] * len(future_dates),
      'moving_avg_7': [group['price'].iloc[-7:].mean()] * len(future_dates),
    })
    X_future = future_data[['rating', 'day', 'month', 'day_of_week', 'days_since_start', 'moving_avg_3', 'moving_avg_7']]
    future_data['predicted_price'] = model.predict(X_future)
    future_data['product_id'] = product_id
    predictions_list.append(future_data)

  try:
    # Insertar predicciones futuras
    for future_data in predictions_list:
      for _, row in future_data.iterrows():
        cursor.execute("""
        INSERT INTO predictions (product_id, timestamp, predicted_price, day, month, day_of_week, days_since_start, rating, moving_avg_3, moving_avg_7)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['product_id'], row['timestamp'], row['predicted_price'], row['day'], row['month'], row['day_of_week'], row['days_since_start'], row['rating'], row['moving_avg_3'], row['moving_avg_7']))
    
    # Insertar errores
    for error in errors_list:
      cursor.execute("""
      INSERT INTO model_errors (product_id, mae, rmse)
      VALUES (%s, %s, %s)
      """, (error['product_id'], error['mae'], error['rmse']))
    
    connection.commit()
    print("Predicciones y errores almacenados correctamente.")
  except Exception as e:
    connection.rollback()
    print(f"Error al guardar predicciones y errores: {e}")
  finally:
    connection.close()

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