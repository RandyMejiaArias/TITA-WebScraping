import requests
from bs4 import BeautifulSoup
from datetime import datetime
from database import output_collection

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

            output_collection.insert_one(product_data)

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