import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from utils.database import output_collection, connection, cursor

# Configuración de headers para scraping
HEADERS = {
    "accept-language": "en-US,en;q=0.9",
    "accept-encoding": "gzip, deflate, br",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
}

def scrape_and_store(url, id_product):
    """
    Realiza el scraping de una URL y guarda los datos obtenidos en MongoDB.
    Implementa lógica de reintento hasta un máximo de intentos.
    """
    attempts = 0
    max_attempts = 6

    while attempts < max_attempts:
        try:
            resp = requests.get(url, headers=HEADERS)
            soup = BeautifulSoup(resp.text, 'html.parser')

            # Obtener el título
            title = soup.find('span', {'id': 'productTitle'}).text.strip()

            # Obtener la calificación de estrellas y truncar a los primeros 3 caracteres
            rating = soup.find('span', {'id': 'acrPopover'})['title'][:3]

            # Obtener el precio y eliminar el signo de $ y el prefijo 'US'
            price = soup.find("span", {"class": "a-price"}).find("span").text.replace('US', '').replace('$', '').strip()

            # Registrar la fecha y hora de la ejecución
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Guardar resultados en MongoDB
            product_data = {
                "product_id": id_product,
                "title": title,
                "rating": rating,
                "price": price,
                "url": url,
                "timestamp": timestamp
            }

            # Convertir el timestamp del día a un objeto datetime
            start_of_day = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').replace(hour=0, minute=0, second=0)
            end_of_day = start_of_day + timedelta(days=1)

            # Convertir los límites de tiempo a cadenas en el mismo formato
            start_of_day_str = start_of_day.strftime('%Y-%m-%d %H:%M:%S')
            end_of_day_str = end_of_day.strftime('%Y-%m-%d %H:%M:%S')

            # Consulta para verificar el mismo día
            product_found = output_collection.find_one({
                "product_id": id_product,
                "timestamp": {
                    "$gte": start_of_day_str,
                    "$lt": end_of_day_str
                }
            })

            if product_found:
                print(f"El producto con idProducto {id_product} y fecha {timestamp} ya estaba registrado en MongoDB.")
                return True

            result = output_collection.insert_one(product_data)
            mongo_id = result.inserted_id 

            # Guardar resultados en MySQL
            mysql_query = """
                INSERT INTO `scraping-data` (product_id, title, rating, price, url, timestamp, _id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            mysql_data = (id_product, title, rating, price, url, timestamp, str(mongo_id))
            cursor.execute(mysql_query, mysql_data)
            connection.commit()

            print(f"Ejecución exitosa. Datos guardados en MongoDB para idProducto {id_product}")
            print("Título:", title)
            print("Calificación:", rating)
            print("Precio:", price)
            print("Fecha y hora:", timestamp)
            return True  # Indica que la ejecución fue exitosa

        except Exception as e:
            attempts += 1
            print(f"Intento {attempts} para idProducto {id_product}: Ocurrió un error ({str(e)}). Reintentando...")
    
    print(f"No se pudo obtener la información para idProducto {id_product} después de {max_attempts} intentos.")
    return False  # Indica que la ejecución falló