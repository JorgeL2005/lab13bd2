import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError

class MongoDBHandler:
    def __init__(self):
        self.mongo_uri = "mongodb://localhost:27017/"
        self.db_name = "lab13"  
        self.categories_name = "categories"
        self.products_name = "products"
        self.client = None
        self.db = None
        self.connect_mongo()

    def connect_mongo(self):
        """Conectar a MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            print(f"Conectado a la base de datos '{self.db_name}' en MongoDB.")
            return True
        except PyMongoError as e:
            print(f"Error al conectar a MongoDB: {e}")
            return False

    def is_connected(self):
        """Verificar si estamos conectados a MongoDB"""
        return self.client is not None and self.db is not None

    def fetch_json_data(self, url):
        """Obtener datos JSON desde una URL"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as errh:
            print(f"HTTP Error al obtener {url}: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Error de conexión al obtener {url}: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Timeout al obtener {url}: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Error al obtener {url}: {err}")
        except ValueError as e:
            print(f"Error al decodificar JSON de {url}: {e}")
        return None

    def fetch_all_categories(self, base_url):
        """Cargar todas las categorías desde la API"""
        if not self.is_connected():
            print("No hay conexión a MongoDB")
            return False
        
        categories_data = self.fetch_json_data(base_url)
        if not categories_data:
            print("No se pudieron obtener las categorías")
            return False
        
        try:
            categories_col = self.db[self.categories_name]
            categories_col.delete_many({})
            
            
            categories_docs = []
            for category in categories_data:
                # Extraemos el slug y name del diccionario
                slug = category.get('slug', '')
                name = category.get('name', slug.replace('-', ' ').title())
                
                categories_docs.append({
                    "name": name,
                    "slug": slug,
                    "products_url": f"https://dummyjson.com/products/category/{slug}?limit=0"
                })
            
            result = categories_col.insert_many(categories_docs)
            print(f"Insertadas {len(result.inserted_ids)} categorías")
            return True
        except PyMongoError as e:
            print(f"Error al insertar categorías en MongoDB: {e}")
            return False
        except AttributeError as e:
            print(f"Error al procesar categorías: {e}")
            return False

    def fetch_all_products(self):
        """Cargar todos los productos categoría por categoría"""
        if not self.is_connected():
            print("No hay conexión a MongoDB")
            return False
        
        try:
            categories_col = self.db[self.categories_name]
            products_col = self.db[self.products_name]
            products_col.delete_many({})
            
            total_products = 0
            categories = categories_col.find({})
            
            for category in categories:
                category_name = category["name"]
                category_slug = category["slug"]
                products_url = category["products_url"]
                
                print(f"Obteniendo productos para categoría: {category_name} ({category_slug})")
                products_data = self.fetch_json_data(products_url)
                
                if products_data and "products" in products_data:
                    products = products_data["products"]
                    
                    # Añadir la categoría a cada producto
                    for product in products:
                        product["category"] = category_name
                        product["category_slug"] = category_slug
                        if 'id' in product:
                            product['product_id'] = product.pop('id')
                    
                    result = products_col.insert_many(products)
                    inserted_count = len(result.inserted_ids)
                    total_products += inserted_count
                    print(f"Insertados {inserted_count} productos para {category_name}")
                else:
                    print(f"No se pudieron obtener productos para {category_name}")
            
            print(f"Total de productos insertados: {total_products}")
            return total_products > 0
        except PyMongoError as e:
            print(f"Error al trabajar con MongoDB: {e}")
            return False

# Crear instancia de MongoDBHandler
mongo_handler = MongoDBHandler()

# URL del JSON para las categorías
categories_url = "https://dummyjson.com/products/categories"

# 1. Cargar todas las categorías
if mongo_handler.fetch_all_categories(categories_url):
    # 2. Cargar todos los productos de cada categoría
    mongo_handler.fetch_all_products()
else:
    print("No se pudieron cargar las categorías, abortando carga de productos")
