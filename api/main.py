from fastapi import FastAPI, Query
import hashlib

app = FastAPI(title="DataMart Legacy Product Catalog API", version="1.0.0")

CATEGORIES = ["Electrónica", "Hogar", "Ropa", "Deportes", "Papelería"]
COUNTRIES = ["Colombia", "México", "Perú", "China", "Estados Unidos"]

def classify_product(code: str, description: str = "") -> dict:
    """Genera atributos deterministas de catálogo para cualquier código consultado."""
    clean_code = code.strip().upper()
    desc_upper = (description or f"PRODUCT {clean_code}").strip().upper()
    
    # Reglas heurísticas por palabras clave hacia las 5 categorías oficiales de DataMart
    if any(k in desc_upper for k in ["LIGHT", "LAMP", "CLOCK", "ALARM", "RADIO", "USB", "TORCH", "FAN"]):
        category = "Electrónica"
    elif any(k in desc_upper for k in ["BAG", "SHIRT", "SOCK", "HAT", "APRON", "PURSE", "SCARF", "UMBRELLA"]):
        category = "Ropa"
    elif any(k in desc_upper for k in ["PEN", "PENCIL", "NOTEBOOK", "CARD", "PAPER", "STICKER", "TAPE", "BOOK"]):
        category = "Papelería"
    elif any(k in desc_upper for k in ["BALL", "GAME", "BIKE", "JUMPING", "SPORT", "FLASK", "BOTTLE", "CAMPING"]):
        category = "Deportes"
    else:
        category = "Hogar"

    h = int(hashlib.md5(clean_code.encode()).hexdigest(), 16)
    return {
        "stock_code": clean_code,
        "normalized_name": desc_upper,
        "category": category,
        "supplier_country": COUNTRIES[h % len(COUNTRIES)],
        "is_active": (h % 10) != 0  # 90% activos, 10% descontinuados
    }

# Catálogo semilla en memoria para el endpoint paginado
SEED_CODES = ["85123A", "71053", "84406B", "84029G", "84029E", "22752", "21730", "22633", "22632", "84879"]
CATALOG = {c: classify_product(c) for c in SEED_CODES}

@app.get("/products")
def list_products(page: int = Query(1, ge=1), size: int = Query(50, ge=1, le=500)):
    items = list(CATALOG.values())
    start = (page - 1) * size
    end = start + size
    return {
        "page": page,
        "size": size,
        "total": len(items),
        "items": items[start:end]
    }

@app.get("/products/{stock_code}")
def get_product(stock_code: str, description: str = Query("")):
    clean_code = stock_code.strip().upper()
    if clean_code not in CATALOG:
        CATALOG[clean_code] = classify_product(clean_code, description)
    return CATALOG[clean_code]
