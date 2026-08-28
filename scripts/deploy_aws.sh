#!/usr/bin/env bash
# =============================================================================
# ETAPA 5 - Deploy AWS do projeto Hydra (escopo: Cafe Arabica / Sul de Minas)
#
# Fluxo:
#   1. Cria o bucket S3 (se inexistente).
#   2. Sincroniza o Data Lake local para o bucket (awss3 sync).
#   3. Faz build da imagem Docker (Streamlit :8501) e sobe o container.
#
# Pre-requisitos:
#   - AWS CLI configurada (aws configure) e docker instalado.
#   - Variaveis S3_BUCKET_RAW/S3_BUCKET_PROCESSED no .env (opcional).
#
# Uso:
#   bash scripts/deploy_aws.sh                 # usa defaults
#   S3_BUCKET=meu-bucket bash scripts/deploy_aws.sh
# =============================================================================
set -euo pipefail

# --- Leitura do .env (best effort) -------------------------------------------
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

REGION="${AWS_REGION:-us-east-1}"
BUCKET_RAW="${S3_BUCKET_RAW:-agro-intel-raw}"
BUCKET_PROCESSED="${S3_BUCKET_PROCESSED:-agro-intel-processed}"
LAKE_DIR="${DATA_LAKE_ROOT:-data_lake}"
EC2_IP="${EC2_IP:-}"   # opcional: IP publico da instancia para deploy remoto

log() { printf '\033[1;34m[Hydra]\033[0m %s\n' "$*"; }

# --- 1. Buckets S3 -----------------------------------------------------------
log "Criando buckets S3 (se necessario)..."
aws s3api head-bucket --bucket "$BUCKET_RAW" 2>/dev/null || \
    aws s3 mb "s3://$BUCKET_RAW" --region "$REGION"
aws s3api head-bucket --bucket "$BUCKET_PROCESSED" 2>/dev/null || \
    aws s3 mb "s3://$BUCKET_PROCESSED" --region "$REGION"

# --- 2. Sync do Data Lake ----------------------------------------------------
log "Sincronizando $LAKE_DIR -> s3://$BUCKET_RAW (camadas raw/processed/gold)..."
aws s3 sync "$LAKE_DIR" "s3://$BUCKET_RAW" \
    --exclude ".gitkeep" \
    --exclude "*.meta.json" \
    --exclude "hydra.db"

# --- 3. Build e subida do container ------------------------------------------
log "Build da imagem Docker..."
docker build -t hydra:latest .

# Deploy remoto (quando EC2_IP informado) via ssh + docker.
if [ -n "${EC2_IP}" ]; then
    log "Deploy remoto em ${EC2_IP}..."
    docker save hydra:latest | ssh -o StrictHostKeyChecking=accept-new \
        "ubuntu@${EC2_IP}" "docker load && docker compose up -d --force-recreate"
else
    log "Subindo container local (docker compose)..."
    docker compose up -d --build
fi

log "Pronto! Painel: http://localhost:8501  (ou http://${EC2_IP}:8501)"
