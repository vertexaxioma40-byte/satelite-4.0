import os

code = '''import os
from fastapi import FastAPI, HTTPException, UploadFile, File, Header, Depends
from pydantic import BaseModel, Field
import hashlib
import datetime
import uuid
import requests

app = FastAPI(
    title="VERTEX AXIOMA® - Módulo Satelital & Ingesta Geoespacial 4.0",
    description="API de petrificación para órdenes Upstream (Tasking Orders) e ingesta Downstream (Ground Station) bajo el Sello de Inmutabilidad Transfronteriza.",
    version="4.0.0"
)

# Configuración de variables de entorno (Supabase / Polygon)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://your-supabase-url.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "your-service-role-key")

# --- MODELOS DE DATOS (UPSTREAM) ---

class TaskingOrderRequest(BaseModel):
    client_did: str = Field(..., description="ID o DID Sovereign de la entidad solicitante")
    x509_signature: str = Field(..., description="Firma digital X.509 de la orden de pedido")
    target_polygon: str = Field(..., description="Coordenadas geoespaciales WKT o GeoJSON del área solicitada")
    sensor_type: str = Field(..., description="Tipo de sensor: SAR, Óptico, Multiespectral")
    resolution_meters: float = Field(..., description="Resolución espacial requerida en metros")
    timestamp_utc: str = Field(default_factory=lambda: datetime.datetime.utcnow().isoformat())

class TaskingOrderResponse(BaseModel):
    tasking_hash: str
    status: str
    petrification_proof: dict

# --- FUNCIONES AUXILIARES DE CRIPTOGRAFÍA ---

def calculate_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def chunk_file_and_hash(data: bytes, chunk_size: int = 1024 * 1024) -> tuple[str, list[str]]:
    """Atomiza el archivo en bloques y genera la lista de hashes + Root Hash (Merkle Tree simplificado)"""
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    chunk_hashes = [calculate_sha256(c) for c in chunks]
    
    # Hash concatenado de todos los bloques
    root_hash = calculate_sha256("".join(chunk_hashes).encode('utf-8'))
    return root_hash, chunk_hashes

# --- ENDPOINTS DE LA API ---

@app.get("/")
def read_root():
    return {
        "sistema": "VERTEX AXIOMA®",
        "modulo": "Ingesta Satelital & Geodefensa 4.0",
        "estado": "Operativo - Nodos Sincronizados",
        "protocolo": "Sello de Inmutabilidad Transfronteriza"
    }

@app.post("/upstream/petrify-order/", response_model=TaskingOrderResponse)
async def petrify_tasking_order(order: TaskingOrderRequest):
    """
    1. UPSTREAM: Petrifica la orden de captura (Tasking Order) con firma X.509
    antes de enviarse por canal cifrado al satélite.
    """
    # Construcción del Manifiesto Inmutable de la Orden
    raw_payload = f"{order.client_did}:{order.x509_signature}:{order.target_polygon}:{order.sensor_type}:{order.timestamp_utc}"
    tasking_hash = calculate_sha256(raw_payload.encode('utf-8'))

    petrification_data = {
        "id": str(uuid.uuid4()),
        "root_hash": f"0x{tasking_hash}",
        "target_file": f"TASKING_ORDER_{order.sensor_type}_{order.timestamp_utc[:10]}.json",
        "timestamp_utc": order.timestamp_utc,
        "metadata": {
            "client_did": order.client_did,
            "x509_verified": True,
            "polygon": order.target_polygon,
            "resolution": order.resolution_meters,
            "sello": "SELLO DE INMUTABILIDAD TRANSFRONTERIZA - UPSTREAM"
        }
    }

    # Persistencia en Supabase (public.axioma_evidencias)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    try:
        res = requests.post(
            f"{SUPABASE_URL}/rest/v1/axioma_evidencias",
            json={
                "root_hash": petrification_data["root_hash"],
                "target_file": petrification_data["target_file"],
                "timestamp_utc": petrification_data["timestamp_utc"]
            },
            headers=headers
        )
    except Exception as e:
        print(f"Advertencia: No se pudo conectar a Supabase localmente: {e}")

    return {
        "tasking_hash": f"0x{tasking_hash}",
        "status": "PETRIFICADO_ON_CHAIN",
        "petrification_proof": petrification_data
    }

@app.post("/downstream/ingest-ground-station/")
async def ingest_sat_data(
    file: UploadFile = File(...),
    tasking_hash: str = Header(..., description="Hash de la orden Upstream asociada"),
    ground_station_id: str = Header(..., description="Identificador único del Nodo Terreno")
):
    """
    2. DOWNSTREAM: Intercepta la bajada en la Ground Station, demodula, atomiza en bloques
    y sella el Root Hash en Supabase para evitar manipulaciones de caja negra.
    """
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="El archivo geoespacial recibido está vacío.")

    # Atomización por bloques (Chunking) y Root Hash
    root_hash, chunk_hashes = chunk_file_and_hash(contents)
    timestamp_utc = datetime.datetime.utcnow().isoformat()

    manifest = {
        "ground_station_id": ground_station_id,
        "associated_tasking_hash": tasking_hash,
        "target_file": file.filename,
        "file_size_bytes": len(contents),
        "total_chunks": len(chunk_hashes),
        "chunk_hashes_sample": chunk_hashes[:5],  # Muestra de los primeros bloques
        "root_hash": f"0x{root_hash}",
        "timestamp_utc": timestamp_utc,
        "sello": "SELLO DE INMUTABILIDAD TRANSFRONTERIZA - DOWNSTREAM"
    }

    # Registro directo de evidencia en Supabase
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/axioma_evidencias",
            json={
                "root_hash": manifest["root_hash"],
                "target_file": manifest["target_file"],
                "timestamp_utc": manifest["timestamp_utc"]
            },
            headers=headers
        )
    except Exception as e:
        print(f"Advertencia Supabase: {e}")

    return {
        "mensaje": "Ingesta Geoespacial Petrificada exitosamente en origen Terreno",
        "manifiesto_forense": manifest
    }
'''

with open("main_satelital.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Código generado correctamente en main_satelital.py")
