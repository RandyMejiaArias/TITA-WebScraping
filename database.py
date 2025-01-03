from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pymysql

# Cargar variables de entorno
load_dotenv()

# Configuración de MongoDB
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_INPUT_COLLECTION = os.getenv("MONGO_INPUT_COLLECTION", "products")
MONGO_OUTPUT_COLLECTION = os.getenv("MONGO_OUTPUT_COLLECTION", "scraping-data")

# Conexión a MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# Colecciones
input_collection = db[MONGO_INPUT_COLLECTION]
output_collection = db[MONGO_OUTPUT_COLLECTION]

# Configuración de MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Conexión a MySQL
config = {
  'user': MYSQL_USER,
  'password': MYSQL_PASSWORD,
  'host': MYSQL_HOST,  # O usa '127.0.0.1'
  'port': 3306,
  'database': MYSQL_DATABASE
}

try:
  # Crear conexión
  connection = pymysql.connect(**config)
  print("Conexión exitosa a la base de datos")

  # Crear un cursor para ejecutar queries
  cursor = connection.cursor()

except pymysql.MySQLError as err:
  print(f"Error: {err}")

# Función para cerrar la conexión MySQL
def close_mysql_connection():
  cursor.close()
  connection.close()
