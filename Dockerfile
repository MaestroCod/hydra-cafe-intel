# ETAPA 5 - Imagem da aplicacao Hydra (Streamlit + motor de dados)
# Python 3.12-slim: wheels manylinux de rasterio/netCDF4/geopandas incluem
# GDAL/GEOS, sem necessidade de compilar no container.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true

WORKDIR /app

# Dependencias de sistema minimas (curl para healthcheck, libgomp para numpy).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Camada de dependencias Python (cache eficiente no build).
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Codigo e artefatos do projeto.
COPY . .

# O lake local e montado como volume (persistencia na EC2).
VOLUME ["/app/data_lake"]

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py"]
