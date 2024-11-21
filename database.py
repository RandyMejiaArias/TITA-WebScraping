from pymongo import MongoClient
import os
from dotenv import load_dotenv

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
