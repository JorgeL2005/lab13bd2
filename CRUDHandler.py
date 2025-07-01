import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo import ASCENDING, DESCENDING
from bson.objectid import ObjectId

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
        
    def crear_indice(self, campo="nombre", coleccion="products"):
        """Crear índice en un campo específico"""
        try:
            collection = self.db[coleccion]
            collection.create_index([(campo, ASCENDING)])
            print(f"Índice creado en el campo '{campo}' de la colección '{coleccion}'")
            return True
        except PyMongoError as e:
            print(f"Error al crear índice: {e}")
            return False

    def crear_producto(self, producto_data, coleccion="products"):
        """Crear un nuevo producto"""
        try:
            collection = self.db[coleccion]
            result = collection.insert_one(producto_data)
            print(f"Producto creado con ID: {result.inserted_id}")
            return result.inserted_id
        except PyMongoError as e:
            print(f"Error al crear producto: {e}")
            return None

    def obtener_productos(self, filtro={}, coleccion="products"):
        """Obtener todos los productos que coincidan con el filtro"""
        try:
            collection = self.db[coleccion]
            return list(collection.find(filtro))
        except PyMongoError as e:
            print(f"Error al obtener productos: {e}")
            return []

    def obtener_producto(self, filtro, coleccion="products"):
        """Obtener un producto específico"""
        try:
            collection = self.db[coleccion]
            return collection.find_one(filtro)
        except PyMongoError as e:
            print(f"Error al obtener producto: {e}")
            return None

    def actualizar_producto(self, filtro, nuevos_valores, coleccion="products"):
        """Actualizar un producto existente"""
        try:
            collection = self.db[coleccion]
            result = collection.update_one(filtro, {"$set": nuevos_valores})
            print(f"Productos actualizados: {result.modified_count}")
            return result.modified_count
        except PyMongoError as e:
            print(f"Error al actualizar producto: {e}")
            return 0

    def eliminar_producto(self, filtro, coleccion="products"):
        """Eliminar un producto"""
        try:
            collection = self.db[coleccion]
            result = collection.delete_one(filtro)
            print(f"Productos eliminados: {result.deleted_count}")
            return result.deleted_count
        except PyMongoError as e:
            print(f"Error al eliminar producto: {e}")
            return 0

    # CONSULTAS BÁSICAS
    
    def obtener_productos_por_precio(self, precio, coleccion="products"):
        """Obtener productos por precio exacto"""
        try:
            collection = self.db[coleccion]
            return list(collection.find({"price": precio}))
        except PyMongoError as e:
            print(f"Error al obtener productos por precio: {e}")
            return []

    def obtener_productos_por_nombre(self, texto, coleccion="products"):
        """Buscar productos que contengan texto en el nombre (búsqueda parcial)"""
        try:
            collection = self.db[coleccion]
            return list(collection.find({"title": {"$regex": texto, "$options": "i"}}))
        except PyMongoError as e:
            print(f"Error al buscar productos por nombre: {e}")
            return []

    # CONSULTAS AGREGADAS
    
    def precio_promedio(self, coleccion="products"):
        """Calcular el precio promedio de todos los productos"""
        try:
            collection = self.db[coleccion]
            pipeline = [
                {"$group": {
                    "_id": None,
                    "precio_promedio": {"$avg": "$price"}
                }}
            ]
            result = list(collection.aggregate(pipeline))
            return round(result[0]["precio_promedio"], 2) if result else 0
        except PyMongoError as e:
            print(f"Error al calcular precio promedio: {e}")
            return 0

    def contar_productos(self, coleccion="products"):
        """Contar el total de productos"""
        try:
            collection = self.db[coleccion]
            return collection.count_documents({})
        except PyMongoError as e:
            print(f"Error al contar productos: {e}")
            return 0

    def mayor_stock_categoria(self, coleccion="products"):
        """Obtener el producto con mayor stock por categoría"""
        try:
            collection = self.db[coleccion]
            pipeline = [
                {"$sort": {"stock": DESCENDING}},
                {"$group": {
                    "_id": "$category",
                    "producto": {"$first": "$$ROOT"},
                    "max_stock": {"$max": "$stock"}
                }},
                {"$project": {
                    "categoria": "$_id",
                    "producto": "$producto.title",
                    "stock": "$max_stock",
                    "_id": 0
                }}
            ]
            return list(collection.aggregate(pipeline))
        except PyMongoError as e:
            print(f"Error al obtener productos con mayor stock: {e}")
            return []

# Crear instancia de MongoDBHandler
mongo_handler = MongoDBHandler()

# Crear índice en campo específico
mongo_handler.crear_indice(campo="title")  # Cambiado a "title" que es el campo real en la API

# Crear productos (ejemplo con datos de la API dummyjson)
mongo_handler.crear_producto({"title": "iPhone 9", "price": 549, "stock": 94, "category": "smartphones"})
mongo_handler.crear_producto({"title": "iPhone X", "price": 899, "stock": 34, "category": "smartphones"})
mongo_handler.crear_producto({"title": "Samsung Universe 9", "price": 1249, "stock": 36, "category": "smartphones"})
mongo_handler.crear_producto({"title": "OPPOF19", "price": 280, "stock": 123, "category": "smartphones"})
mongo_handler.crear_producto({"title": "Huawei P30", "price": 499, "stock": 32, "category": "smartphones"})
mongo_handler.crear_producto({"title": "MacBook Pro", "price": 1749, "stock": 83, "category": "laptops"})
mongo_handler.crear_producto({"title": "Samsung Galaxy Book", "price": 1499, "stock": 50, "category": "laptops"})

# Leer productos
productos = mongo_handler.obtener_productos()
print("\nLista de productos:", [p["title"] for p in productos])

# Leer producto específico
if productos:
    producto = mongo_handler.obtener_producto({"_id": productos[0]['_id']})
    print("\nProducto obtenido:", producto["title"])

# Actualizar un producto
if productos:
    resultado = mongo_handler.actualizar_producto(
        {"_id": productos[0]['_id']},
        {"price": 1200}
    )
    print(f"\nProductos actualizados: {resultado}")

# Eliminar un producto
if productos:
    resultado = mongo_handler.eliminar_producto({"_id": productos[0]['_id']})
    print(f"\nProductos eliminados: {resultado}")

# Consultas básicas
print("\nProductos con precio 549:", [p["title"] for p in mongo_handler.obtener_productos_por_precio(549)])
print("\nProductos que contienen 'iPhone' en el nombre:", 
      [p["title"] for p in mongo_handler.obtener_productos_por_nombre("iPhone")])

# Consultas agregadas
print("\nPrecio promedio de productos:", mongo_handler.precio_promedio())
print("\nTotal de productos:", mongo_handler.contar_productos())
print("\nEl producto con mayor stock por categoría:", mongo_handler.mayor_stock_categoria())
