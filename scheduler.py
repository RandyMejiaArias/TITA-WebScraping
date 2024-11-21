import schedule
import time
from database import input_collection
from scraper import scrape_and_store
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

# Horarios desde variables de entorno
EXECUTION_TIME_1 = os.getenv("EXECUTION_TIME_1", "06:00")
EXECUTION_TIME_2 = os.getenv("EXECUTION_TIME_2", "18:00")

def process_documents():
    print("Iniciando proceso de scraping...")
    try:
        # Obtener documentos desde la base de datos
        documentos = input_collection.find()  # Ajusta el query si necesitas filtrar datos específicos

        for doc in documentos:
            url = doc.get("url")
            id_product = doc.get("idProduct")

            if url and id_product:
                success = scrape_and_store(url, id_product)
                if not success:
                    print(f"El scraping falló para e producto {id_product}. Verifica la URL: {url}")
            else:
                print(f"Documento inválido: {doc}")

    except Exception as e:
        print("Error al procesar documentos:", str(e))

# Programar las ejecuciones
schedule.every().day.at(EXECUTION_TIME_1).do(process_documents)
schedule.every().day.at(EXECUTION_TIME_2).do(process_documents)

def start_scheduler():
    print("Iniciando programador...")
    while True:
        schedule.run_pending()
        time.sleep(1)
