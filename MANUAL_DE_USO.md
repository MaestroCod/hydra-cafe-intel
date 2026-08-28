# Hydra — Manual de Uso Operacional (Ponta a Ponta)

> Plataforma de Inteligência Climática e Financeira para o Agronegócio
> Escopo atual: café arábica tipo 4/5 nos polos **Sul de Minas** e **Cerrado Mineiro** (Brasil).
> Granularidade de entrega: **semanal** (Gold) · Histórico: **1 ano** · Frequência de atualização: **diária** (raw) / **semanal** (Gold).

Este manual descreve o fluxo completo: instalação → configuração → ingestão →
processamento → camada Gold → banco relacional → dashboard → deploy AWS → qualidade.

---

## 1. Visão geral da arquitetura

```
                          ┌────────────────────────────────────────────┐
                          │            FONTES EXTERNAS                 │
                          │  Yahoo Finance · CHIRPS (UCSB) · CDS/ERA5   │
                          └───────────────┬───────────────┬─────────────┘
                                          ▼               ▼
                     ┌─────────────────────────────────────────────────┐
                     │            INGESTÃO  (camada raw)               │
                     │  raw/finance/…          raw/climate_chirps/…    │
                     │  raw/climate_era5/…                             │
                     └───────────────────────┬─────────────────────────┘
                                             ▼
                     ┌─────────────────────────────────────────────────┐
                     │  PROCESSAMENTO  (camada processed / silver)     │
                     │  - estatística zonal (polos produtores)         │
                     │  - balanço hídrico FAO-56                       │
                     │  - cotação BRL/saca + retorno + vol 21d (×√252) │
                     └───────────────────────┬─────────────────────────┘
                                             ▼
                     ┌─────────────────────────────────────────────────┐
                     │  GOLD  (camada analítica semanal)               │
                     │  analytics_coffee_stress_weekly.parquet         │
                     └──────────────┬──────────────────┬───────────────┘
                                    ▼                  ▼
                     ┌───────────────────────┐  ┌───────────────────────┐
                     │  Streamlit (app.py)   │  │  PostgreSQL / SQLite  │
                     │  painel :8501         │  │  (sync_weekly_gold)   │
                     └───────────────────────┘  └───────────────────────┘
```

Camadas seguem a **Medallion Architecture** com particionamento Hive (`dt=YYYY-MM-DD`),
compatível com Athena/Glue no S3.

### Polos produtores cobertos
| Polo | Sigla | Escopo |
| --- | --- | --- |
| Sul de Minas | `sul_minas` | Café arábica (cinturão produtor principal) |
| Cerrado Mineiro | `cerrado_mineiro` | Café arábica (região do Cerrado de MG) |

> Os polígonos vivem em `src/processing/geometry.py` (GeoJSON embutido) e a malha
> de pontos dos polos é derivada internamente.

---

## 2. Pré-requisitos

| Requisito | Versão mínima | Motivo |
| --- | --- | --- |
| Python | **3.12** | código + dependências testadas |
| pip / venv | — | isolamento de dependências |
| GDAL + libs raster | funcional | `rasterio`/`rioxarray` (clip e zonal stats) |
| HDF5 / NetCDF | funcional | leitura de arquivos `.nc` do ERA5 |
| Internet | — | Yahoo Finance, CHIRPS e CDS |
| Conta CDS (Copernicus) | — | download ERA5-Land (opcional mas recomendado) |
| Docker + AWS CLI | — | somente para a Etapa 9 (deploy) |

> O `setup_environment.py` valida GDAL, HDF5, credenciais e conectividade
> automaticamente — não é preciso checar nada à mão.

---

## 3. Instalação (uma única vez)

### 3.1 Criar ambiente virtual e instalar dependências

```powershell
# Na raiz do projeto
python -m venv .venv
.\.venv\Scripts\Activate.ps1          # Windows
# source .venv/bin/activate          # Linux/macOS

pip install --upgrade pip
pip install -r requirements.txt
pip install -e .                     # instala o pacote `agro_intel` em modo editável
```

> Se usar o lockfile reproduzível: `pip install -r requirements-lock.txt`
> (todas as versões fixadas e já testadas juntas).

### 3.2 Bootstrap do ambiente (pastas + validação)

```powershell
python setup_environment.py
# Códigos de saída:
#   0 = ambiente pronto
#   1 = falha bloqueante (dependência/pasta ausente)
#   2 = pronto com pendências opcionais (credencial/Chave)

# Testar conectividade com as fontes externas:
python setup_environment.py --network-check

# Gerar o ~/.cdsapirc a partir do .env (chave CDS):
python setup_environment.py --write-cdsapirc
```

### 3.3 Configuração do `.env`

Copie `.env.example` para `.env` e preencha:

```env
# --- Armazenamento -----------------------------------------------------------
STORAGE_BACKEND=local          # local | s3
DATA_LAKE_ROOT=data_lake

# --- Eixo financeiro (Yahoo Finance) -----------------------------------------
FINANCE_TICKERS=KC=F,BRL=X     # café arábica ICE + câmbio
FINANCE_START_DATE=1y

# --- Eixo climático -----------------------------------------------------------
CHIRPS_LOOKBACK_DAYS=380       # janela de precipitação (dias)
ERA5_LOOKBACK_DAYS=395         # janela de reanálise (dias)
LATITUDE_DEFAULT=-21.8         # fallback para grade fora dos polos
LONGITUDE_DEFAULT=-46.5

# --- ERA5 / CDS (Copernicus) --------------------------------------------------
CDS_URL=https://cds.climate.copernicus.eu/api
CDS_KEY=<uid>:<api-key>        # gerada em cds.climate.copernicus.eu (licença aceita)

# --- Banco relacional ---------------------------------------------------------
DB_BACKEND=sqlite              # sqlite | postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=agro_intel
POSTGRES_USER=postgres
POSTGRES_PASSWORD=
## 4. Etapas de ingestão (camada raw)

### 4.1 Financeiro — Yahoo Finance

```powershell
# Hoje com o ticker padrão (KC=F + BRL=X) e histórico configurado:
python -m src.ingestion.finance

# Janela / tickers específicos:
python -m src.ingestion.finance --period 1mo
python -m src.ingestion.finance --start 2025-08-01 --tickers KC=F,BRL=X
```

Destino: `raw/finance/ticker_safe=KC_F/dt=YYYY-MM-DD/KC=F_1d.parquet`

### 4.2 CHIRPS — precipitação diária

```powershell
# Últimos dias (latência ~1 mês no produto final; `--prelim` para quase tempo real):
python -m src.ingestion.chirps
python -m src.ingestion.chirps --date 2026-07-15
python -m src.ingestion.chirps --start 2026-07-01 --end 2026-07-05
```

## 5. Processamento raw → processed (silver)

```powershell
# Fluxo completo (clima + financeiro):
python -m src.processing.pipeline

# Apenas um eixo:
python -m src.processing.pipeline --climate-only
python -m src.processing.pipeline --finance-only

# Janela climática específica / limitar histórico financeiro:
python -m src.processing.pipeline --start 2025-08-01 --end 2026-08-01
python -m src.processing.pipeline --finance-only --finance-max-days 400
```

O que o pipeline produz:

| Saída | Caminho | Conteúdo |
| --- | --- | --- |
| Balanço hídrico | `processed/climate/water_balance/dt=*/…parquet` | precipitação zonal, tmax/tmin, ETP, déficit, `dados_completos` |
| Cotação + risco | `processed/finance/cotacoes_brl_saca/ticker=KC=F/…parquet` | `preco_brl_saca`, `retorno_diario_pct`, `volatilidade_21d_anualizada` |

**Métricas de risco financeiro (camada processed):**

- `retorno_diario_pct = (P_t / P_{t-1} − 1) × 100` (fechamento convertido por `BRL=X`)
- `volatilidade_21d_diaria` = desvio padrão amostral (`ddof=1`) dos retornos diários
  em janela móvel de **21 pregões**, com `min_periods=5`
## 6. Camada Gold (analítica semanal)

```powershell
# Gerar / sobrescrever a Gold semanal:
python -m src.analytics.gold --weekly
```

Saída: `gold/analytics_coffee_stress_weekly.parquet`

| Coluna | Descrição |
| --- | --- |
| `data_semana` | Domingo (W-SUN) que abre a semana |
| `polo` | `sul_minas` / `cerrado_mineiro` |
| `precipitacao_semanal_mm` | Σ precipitação CHIRPS da semana |
| `et0_semanal_mm` | Σ ETP (ERA5 `pev` ou Hargreaves) |
| `deficit_hidrico_semanal` | Σ (precipitação − ETP) |
| `crop_stress_index` | `max(−déficit, 0)` — positivo = estresse hídrico |
| `alerta_estresse` | `crop_stress_index > 15 mm/semana` |
| `ticker` | `KC=F` |
| `preco_brl_saca` | **último** fechamento BRL/saca da semana |
| `retorno_semanal_pct` | variação % sobre a semana anterior |
## 7. Banco relacional (sync da Gold)

```powershell
# Grava as 47/52 semanas da Gold no banco (idempotente por data+polo+ticker):
python -m src.database.sync
```

- Backend: `DB_BACKEND` do `.env` (`sqlite` local em `data_lake/hydra.db` ou
  `postgres`).
- Tabela `fato_diario` / `fato_semanal` atualizadas por `upsert`
  (`data_semana`, `polo`, `ticker`); reexecutar é seguro.

---

## 8. Dashboard Streamlit (app)

```powershell
streamlit run app.py
# abre em http://localhost:8501
```

KPI cards (seção superior):

| Card | Valor | Delta | Observação |
| --- | --- | --- | --- |
| Preço Café (R$/saca) | `preco_brl_saca` | `retorno_semanal_pct` | última semana Gold |
## 9. Backfill completo de 1 ano (fluxo único)

```powershell
# Fluxo completo: financeiro + CHIRPS + ERA5 + pipeline + Gold + sync:
python scripts/backfill_1y.py

# Somente o plano (sem chamadas externas):
python scripts/backfill_1y.py --dry-run

# Somente o eixo financeiro:
python scripts/backfill_1y.py --finance-only

# Pular o ERA5 (licença pendente) — ETP via fallback Hargreaves:
python scripts/backfill_1y.py --skip-era5

# Não regravar objetos já existentes:
python scripts/backfill_1y.py --no-overwrite
```

Log consolidado em `backfill.log` (em paralelo ao console estruturado).

---

## 10. Deploy AWS / Docker (produção)

### 10.1 Local via Docker Compose

```bash
docker compose up -d --build        # painel em http://localhost:8501
## 11. Qualidade e validação

```powershell
# Suite completa de testes (64 testes):
python -m pytest tests -q
python -m pytest tests -q -k finance        # filtro por tema

# Lint e tipos:
python -m ruff check src scripts app.py tests --output-format concise
python -m ruff format --check src scripts app.py tests
python -m mypy src scripts

# Smoke do app com as colunas atuais da Gold:
python -c "import app; app.render_metrics(app.load_gold_weekly.__wrapped__()); print('cards ok')"
```

Checklist de aceite antes de entregar:

- [ ] `pytest tests` → 64 passed
- [ ] `ruff check` → all checks passed
- [ ] `mypy` → no issues found
- [ ] `python -m src.processing.pipeline --finance-only` → `PIPELINE OK`
- [ ] `python -m src.analytics.gold --weekly` → `GOLD SEMANAL OK` (47+ semanas)
- [ ] `python -m src.database.sync` → `SYNC OK`
- [ ] `streamlit run app.py` → health `ok` e 4 cards renderizando

---

## 12. Estrutura de arquivos e dados

```
.
├── app.py                       # dashboard Streamlit (KPI + gráficos)
├── MANUAL_DE_USO.md             # este documento
├── APRESENTACAO_PROJETO.md      # documentação acadêmica (TCC)
├── data_lake/                   # Data Lake local (medallion)
│   ├── raw/
│   │   ├── finance/ticker_safe=KC_F/dt=*/KC=F_1d.parquet
│   │   ├── climate_chirps/dt=*/chirps_brazil.tif
│   │   └── climate_era5/dt=*/era5_land_brazil.nc
│   ├── processed/
│   │   ├── finance/cotacoes_brl_saca/ticker=KC=F/cotacoes_brl_saca.parquet
## 13. Troubleshooting

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `cdsapi` retorna `HTTP 403` / `Not allowed` | Licença do ERA5-Land não aceita no portal CDS | Aceitar a licença em cds.climate.copernicus.eu; ou usar `--skip-era5` |
| CHIRPS de hoje em branco | Latência de 1–2 meses no produto final | Usar `--prelim` ou data anterior |
| Gold com poucas semanas | CHIRPS/ERA5 com janela menor que 52 semanas | Rodar `python scripts/backfill_1y.py --skip-era5` |
| `volatilidade_21d_anualizada` em `NaN` nas primeiras linhas | Janela 21 pregões ainda não completou (min 5 observações) | Comportamento esperado; ignore as primeiras semanas |
| `ICF=F` (B3) sem dados | Ticker não disponível no Yahoo Finance | **Limitação de fonte**; usar `KC=F` (ICE) como referência |
| `pipeline` gera `NaN` em dias sem clima | Ausência parcial de dados (integridade temporal) | Comportamento esperado; flags `*_disponivel=False` |
| `gdal_translate`/rasterio quebra | GDAL incompatível com o wheel | `pip install --force-reinstall "rasterio>=1.3"` e rodar `setup_environment.py` |
| Streamlit usa muito RAM no t3.micro | Vários gráficos carregados | Container com `mem_limit: 800m` (já no compose) |
| Sync DB sem efeito | `DB_BACKEND` apontando para outro banco | Conferir `.env` e rodar `python -m src.database.sync -v` |

---

## 14. Referência rápida de comandos

```powershell
# Bootstrap
python setup_environment.py --network-check
python setup_environment.py --write-cdsapirc

# Ingestão
python -m src.ingestion.finance
python -m src.ingestion.chirps --date 2026-07-15
python -m src.ingestion.era5 --date 2026-07-15

# Transformação
python -m src.processing.pipeline --finance-only
python -m src.analytics.gold --weekly

# Persistência + dashboard
python -m src.database.sync
streamlit run app.py

# Backfill completo
python scripts/backfill_1y.py --skip-era5

# Qualidade
python -m pytest tests -q
python -m ruff check src scripts app.py tests
python -m mypy src scripts
```

---

> **Observações finais:** (1) `ICF=F` (B3) não está disponível no Yahoo Finance —
> a referência oficial do escopo é `KC=F` (ICE); (2) o ERA5-Land requer aceite
> manual da licença; (3) a Gold semanal é o artefato oficial de entrega e alimenta
> tanto o dashboard quanto o banco relacional.

│   │   └── climate/water_balance/dt=*/water_balance.parquet
│   ├── gold/analytics_coffee_stress_weekly.parquet
│   └── hydra.db                 # SQLite (quando DB_BACKEND=sqlite)
├── src/
│   ├── config.py                # Settings via pydantic + log estruturado
│   ├── storage/                 # backend local/S3 + factory
│   ├── ingestion/               # finance, chirps, era5, retry
│   ├── processing/              # pipeline, water_balance, zonal_stats, finance_transform
│   ├── analytics/gold.py        # agregação semanal + alertas
│   └── database/sync.py         # upsert da Gold no banco
├── scripts/
│   ├── backfill_1y.py           # fluxo completo de 1 ano
│   └── deploy_aws.sh            # deploy S3 + Docker
├── tests/                       # 64 testes (processing, database, storage…)
├── setup_environment.py         # bootstrap do ambiente
├── Dockerfile  ·  docker-compose.yml
├── requirements.txt  ·  requirements-lock.txt  ·  pyproject.toml
└── .env  ·  .env.example  ·  .gitignore
```

---

docker compose ps                   # healthcheck do container `hydra`
```

O `docker-compose.yml` monta `data_lake` como volume e injeta o `.env`
(limite de memória 800m p/ t3.micro).

### 10.2 Deploy para AWS

```bash
bash scripts/deploy_aws.sh                    # buckets + sync + build local
S3_BUCKET=meu-bucket bash scripts/deploy_aws.sh
EC2_IP=1.2.3.4 bash scripts/deploy_aws.sh      # deploy remoto via ssh+docker
```

O script: cria os buckets S3 (se inexistentes), sincroniza o lake
(`raw`/`processed`/`gold`) e sobe o container. Exclui do sync: `.gitkeep`,
`*.meta.json` e `hydra.db`.

---

| Volatilidade Móvel (21d) | `% a.a.` | Δ em p.p. vs. semana anterior | `delta_color="inverse"`: sobe = vermelho (risco) |
| Déficit Hídrico (CSI) | mm | — | `crop_stress_index` |
| Chuva Semanal | mm | — | alerta visual se `< 10 mm` |

Gráficos: série semanal (CSI × preço, eixo duplo), heatmap de correlação
Pearson entre anomalia climática (CSI) e mercado (retorno diário / vol 21d).

Healthcheck: `http://localhost:8501/_stcore/health` → `ok`.

---

| `volatilidade_21d_anualizada` | **snapshot (`last`)** da vol diária anualizada no fim da semana |
| `volatilidade_4w_semanal_anualizada` | std amostral dos retornos semanais (4 semanas, `min_periods=2`) × `√52` |

---

- `volatilidade_21d_anualizada = volatilidade_21d_diaria × √252`

> Datas sem cobertura de alguma fonte **não interrompem** o pipeline: geram linhas
> com `NaN` e flags `*_disponivel=False`, preservando a integridade temporal.

---

Destino: `raw/climate_chirps/dt=YYYY-MM-DD/chirps_brazil.tif` (grade 0,05°, mm/dia)

### 4.3 ERA5-Land — reanálise horária (requer licença aceita)

```powershell
# Um dia:
python -m src.ingestion.era5 --date 2026-07-15

# Ver o payload do CDS sem baixar:
python -m src.ingestion.era5 --dry-run
```

Destino: `raw/climate_era5/dt=YYYY-MM-DD/era5_land_brazil.nc`
Variaveis: `2m_temperature` (horaria; tmax/tmin derivados), `total_precipitation`,
`surface_net_solar_radiation` e `surface_pressure` → ETP de referencia (PEV/FAO-56).

> O recorte espacial (bBox Brasil) é feito **no servidor** do CDS; só o retângulo
> do Brasil trafega. Cada mês de backfill = 1 requisição ao CDS.

---

```

### 3.4 Aceitar a licença do ERA5-Land (bloqueio conhecido)

O dataset ERA5-Land exige **aceite manual da licença** no portal Copernicus
(uma única vez por conta). Enquanto não for aceito, o `cdsapi` falha com
`HTTP 403`. **Workaround:** executar o fluxo com `--skip-era5` (a Gold usa o
fallback Hargreaves para ETP quando `etp_mm` está indisponível).

---
