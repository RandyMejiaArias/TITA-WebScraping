import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from utils.database import output_collection, connection, cursor

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
        # Verificar si el registro ya existe
        cursor.execute("""
        SELECT EXISTS(
          SELECT 1 FROM predictions
          WHERE product_id = %s AND timestamp = %s
        ) AS record_exists
        """, (row['product_id'], row['timestamp']))
        
        result = cursor.fetchone()
        if result is None or result['record_exists'] == 0:
          cursor.execute("""
          INSERT INTO predictions (product_id, timestamp, predicted_price, day, month, day_of_week, days_since_start, rating, moving_avg_3, moving_avg_7)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, (row['product_id'], row['timestamp'], row['predicted_price'], row['day'], row['month'], row['day_of_week'], row['days_since_start'], row['rating'], row['moving_avg_3'], row['moving_avg_7']))
    
    # Insertar errores
    for error in errors_list:
      # Verificar si el registro ya existe
      cursor.execute("""
      SELECT EXISTS(
        SELECT 1 FROM predictions
        WHERE product_id = %s AND timestamp LIKE %s
      ) AS record_exists
      """, (row['product_id'], row['timestamp'].strftime('%Y-%m-%d')))
      result = cursor.fetchone()
      if result is None or result['record_exists'] == 0:
        cursor.execute("""
        INSERT INTO model_errors (product_id, mae, rmse)
        VALUES (%s, %s, %s)
        """, (error['product_id'], error['mae'], error['rmse']))
    
    connection.commit()
    print("Predicciones y errores almacenados correctamente.")
  except Exception as e:
    connection.rollback()
    print(f"Error al guardar predicciones y errores: {e}")

