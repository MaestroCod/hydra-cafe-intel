# Plataforma de Inteligência Climática e Financeira do Café Arábica (Hydra)

**Documento de apresentação do projeto** — Data Lake Medallion (Raw → Processed → Gold)
com ingestão de fontes externas, geoprocessamento, modelagem financeira e analítica.

**Escopo:** apenas **café arábica tipo 4/5** (`KC=F`/`ICF=F`) · polos **Sul de Minas**
e **Cerrado Mineiro (MG)** · histórico de **1 ano** · granularidade **semanal (1W)**.

---

## 1. O Problema

Produtores e mesas de trading de **café arábica** tomam decisões sem uma visão
integrada entre o **clima nos polos produtores** (chuva, temperatura, balanço
hídrico) e o **preço da commodity** (ICE em USD convertido para BRL/saca). O
projeto entrega essa integração de forma automatizada, escalável e observável.



## 2. O Domínio: Café Arábica, Ciclos e Regiões de Referência

A plataforma acompanha **café arábica tipo 4/5** e suas **janelas fenológicas
críticas** (períodos em que o estresse hídrico mais penaliza a produtividade).
Os valores são referências regionais e variam conforme a região e o ano-safra:

| Cultura | Período de plantio | Janela crítica (água) | Colheita |
| --- | --- | --- | --- |
| Café arábica (perene) | — | Floração **set–out** · Granação **jan–mar** | mai–set |

É nessas janelas que o **Crop Stress Index** (déficit hídrico acumulado de 7/14
dias) é cruzado com retorno e volatilidade dos contratos.

### 2.1 Regiões (Polos) de Referência

| Polo | UF | Cultura | Características | BBox WGS84 (W, S, E, N) |
| --- | --- | --- | --- | --- |
| `Sul_de_Minas` | MG | Café arábica | Varginha, Três Corações, Alfenas, Guaxupé; altitude 800–1.200 m | `(-47.00, -22.90, -44.20, -20.60)` |
| `Cerrado_Mineiro` | MG | Café arábica | Alto Paranaíba/Triângulo; Denominação de Origem; irrigação expressiva | `(-48.20, -20.00, -45.60, -17.30)` |

> O motor também mantém os polos de soja/milho (Sorriso-MT, Oeste-PR) como
> catálogo, mas o **escopo ativo** (`SCOPE_POLOS`/`SCOPE_COMMODITIES` no `.env`)
> processa apenas café nos dois polos mineiros.


As geometrias (bounding boxes em **EPSG:4326**, mesma grade do CHIRPS/ERA5) estão
em `src/processing/geometry.py` e podem ser substituídas por polígonos oficiais
(malha do IBGE, por exemplo) via `POLOS_GEOJSON_PATH` no `.env`.

## 3. A Arquitetura (Medallion + Storage Abstraction)

```
FONTES                          DATA LAKE (S3 local simulado)            CONSUMO
────────                        ────────────────────────────────         ──────
Yahoo Finance ──► raw/finance/ ──► processed/finance/ ──► gold/    ──► Painel / APIs
CHIRPS (UCSB) ─► raw/climate_chirps/ ─► processed/climate/ ──┘
ERA5-Land (CDS)► raw/climate_era5/
```

- **Camada `raw`**: bytes imutáveis da fonte (Parquet, GeoTIFF, NetCDF).
- **Camada `processed` (Silver)**: estatísticas zonais por polo produtor, balanço
  hídrico FAO-56 e cotações convertidas para BRL/saca com métricas de risco.
- **Camada `gold`**: tabelas analíticas (correlação estresse hídrico × mercado).
- **Abstração de storage**: todo o código lê/escreve via `StorageBackend`
  (`LocalStorage` hoje; troca para AWS S3 com `STORAGE_BACKEND=s3` no `.env`,
  sem alterar nenhuma linha de código).

## 4. O Que Foi Construído (Etapas 1–4)

### Etapa 1 — Fundação do ambiente
- `requirements.txt`, `.env`/`.env.example`, `.gitignore`, `setup_environment.py`.
- Validação de dependências + smoke tests de GDAL/NetCDF/Parquet.
- Log estruturado em console + arquivo (padrão CloudWatch).

### Etapa 2 — Storage e ingestão financeira
- `src/config.py`: `Settings` tipado e imutável (segredos ocultos no repr).
- `src/storage/`: interface `StorageBackend`, `LocalStorage` (escrita atômica),
  `S3Storage` (boto3, roteamento camada→bucket) e factory `get_storage()`.
- `src/ingestion/finance.py`: cotação de **café arábica** `KC=F`, `ICF=F` e
  câmbio `BRL=X` via yfinance, com metadados de auditoria (`ingested_at`,
  `run_id`) e Parquet particionado `ticker_safe=…/dt=…`.

### Etapa 3 — Ingestão climática
- `src/ingestion/chirps.py`: download HTTP por streaming, gzip em memória,
  recorte `clip_box` para o Brasil e exportação GeoTIFF **LZW** via MemoryFile.
- `src/ingestion/era5.py`: cdsapi com recorte **no servidor** (área N,W,S,E),
  resolução de variáveis (tmax/tmin → 2m_temperature horária, conforme o catálogo
  oficial do CDS), NetCDF validado e temporário sempre removido.
- `src/ingestion/retry.py`: backoff exponencial com predicado `giveup`.

### Etapa 4 — Processamento Silver + Gold
- `src/processing/geometry.py`: catálogo de polos (café + soja/milho) com
  fallback por bounding box ou GeoJSON; **escopo ativo = café** (Sul de Minas e
  Cerrado Mineiro) via `SCOPE_*`.
- `src/processing/zonal_stats.py`: estatística zonal ponderada por `cos(lat)`
  de CHIRPS e ERA5; datas ausentes viram placeholders `NaN` (integridade temporal).
- `src/processing/water_balance.py`: Tmax/Tmin/Tmean diários, ETP (ERA5 `pev`
  ou **Hargreaves-Samani** como fallback), `deficit_hidrico_mm`, déficit
  acumulado 7/14 dias e `alerta_estresse_hidrico`.
- `src/processing/finance_transform.py`: conversão dos contratos internacionais
  para **BRL/saca de 60 kg** (join temporal com `BRL=X`), retornos diários e
  volatilidade móvel de 7 dias.
- `src/processing/pipeline.py`: orquestração Raw → Processed com particionamento
  Hive (`dt=`/`ticker_safe=`) e resiliência a dados parciais.
- `src/analytics/gold.py`: Crop Stress Index + matriz de correlação de Pearson
  (valor-p via beta incompleta, sem dependência de scipy) →
  `gold/analytics_crop_market.parquet`.

## 5. Estado Atual e Validação

| Métrica | Valor |
| --- | --- |
| Módulos Python (src + scripts + testes) | **29 arquivos · ~8.300 linhas** |
| Testes automatizados (offline) | **64 passando** · ruff limpo · mypy limpo |
| Escopo | **Café arábica tipo 4/5** (KC=F/ICF=F) · Sul de Minas + Cerrado Mineiro |
| Histórico / granularidade | **Backfill de 1 ano** · Gold **semanal (1W)** |
| Data Lake local | raw + processed + **gold semanal (52 semanas)** + **SQLite `hydra.db`** |
| Ingestão financeira real | KC=F, ICF=F, BRL=X (1 ano para trás via `FINANCE_START_DATE=1y`) |
| Ingestão climática real | CHIRPS diário + **ERA5 mensal (12 requisições ao CDS)** |
| Painel Streamlit | `streamlit run app.py` — healthcheck **HTTP 200 OK** |

Exemplos de saída real:

```
FIN KC=F | 452,12 USD/saca × USDBRL 5,1373 = 2.322,70 BRL/saca | retorno -5,75% | vol 7d 2,97%
GOLD semanal | 52 semanas, 10 alertas de estresse -> gold/analytics_coffee_stress_weekly.parquet
SYNC        | UPSERT idempotente: dim_polo + 52 clima + 52 cotacoes (SQLite/PostGIS)
BACKFILL    | 365 dias de CHIRPS + 12-13 meses de ERA5 + 1y de cotacoes (scripts/backfill_1y.py)
```

## 6. Guia do Usuário (comandos de execução)

```powershell
.\.venv\Scripts\Activate.ps1
# BACKFILL COMPLETO DE 1 ANO (recomendado): financeiro + CHIRPS + ERA5 + Gold semanal
python scripts/backfill_1y.py --dry-run          # mostra o plano (sem executar)
python scripts/backfill_1y.py --skip-era5        # executa tudo exceto ERA5 (licenca pendente)

# Etapas 1-4 - ingestao individual
python -m src.ingestion.finance                       # cafe + cambio, 1y -> raw
python -m src.ingestion.chirps --lookback-days 365    # CHIRPS 1 ano -> raw
python -m src.ingestion.era5  --backfill-days 365     # ERA5 mensal (12 req. CDS)
python -m src.ingestion.era5  --month 2026-07         # mes unico (requisicao mensal)
python -m src.processing.pipeline                     # raw -> processed
python -m src.analytics.gold --weekly                 # -> gold semanal (52 semanas)
python -m src.database.sync                           # Gold -> banco (RDS/SQLite)

# Painel
streamlit run app.py                                  # http://localhost:8501

# Qualidade
python -m pytest -q ; python -m ruff check src tests ; python -m mypy src
```


### 6.1 Credencial do Copernicus (CDSAPI_KEY)

A ingestão do **ERA5-Land** (Etapa 3) exige um token gratuito do **Copernicus
Climate Data Store (CDS)**:

1. Crie ou conecte sua conta em <https://cds.climate.copernicus.eu> e aceite a
   licença do dataset `reanalysis-era5-land`.
2. Em **Profile → API key**, copie a chave no formato `UID:hash` (ex.:
   `123456:abcdef123456…`).
3. Cole no `.env` do projeto (o arquivo está protegido pelo `.gitignore`):

   ```dotenv
   CDSAPI_URL=https://cds.climate.copernicus.eu/api
   CDSAPI_KEY=SEU_UID:SEU_HASH
   ```

4. Gere o `~/.cdsapirc` automaticamente (usado por padrão pelo cdsapi):

   ```powershell
   python setup_environment.py --write-cdsapirc
   ```

5. Valide o ambiente e faça um teste sem consumir a fila do CDS:

   ```powershell
   python setup_environment.py                        # deve terminar "AMBIENTE 100% PRONTO"
   python -m src.ingestion.era5 --date 2026-08-10 --dry-run   # inspeciona o payload do CDS
   python -m src.ingestion.era5 --date 2026-08-10             # ingestão real
   ```

> **Comportamento sem a chave:** o pipeline continua funcionando com CHIRPS e o
> balanço hídrico usa o **fallback de ETP (Hargreaves)** quando há temperatura.
> Apenas a camada `raw/climate_era5/` permanece vazia até a chave ser configurada.


## 7. Etapa 5 (Final) — Banco Relacional, Gold Semanal e Painel

Escopo Hydra: **Café Arábica (KC=F / ICF=F)** · **Polo Sul de Minas (MG)** ·
**Granularidade semanal (1W)**.

### 7.1 Arquitetura do Medalhão (consolidada)

```
Yahoo Finance ─► raw/finance ─► processed/finance ─► gold/ (diário + semanal) ─► app.py
CHIRPS        ─► raw/climate_chirps ─► processed/climate ─► DimPolo + Fact* ─► PostgreSQL
ERA5-Land     ─► raw/climate_era5 ─┘                        (RDS PostGIS / SQLite fallback)
```

### 7.2 Modelo de dados relacional

| Tabela | Descrição | Chave natural do UPSERT |
| --- | --- | --- |
| `dim_polo` | Polos produtores (bbox + `geometry_wkt` para PostGIS) | `nome` |
| `fact_clima_semanal_cafe` | Precipitação, ET0, déficit e Crop Stress Index semanais | `(data_semana, polo_id)` |
| `fact_cotacoes_cafe_saca` | Preço BRL/saca, retorno e volatilidade semanais | `(data_semana, ticker)` |

### 7.3 O que foi entregue

- `scripts/backfill_1y.py` → **backfill de 1 ano** do escopo café: financeiro
  (`FINANCE_START_DATE=1y`), CHIRPS diário (`--lookback-days 365`), **ERA5 mensal**
  (`--backfill-days 365` = 12–13 requisições ao CDS), pipeline, Gold semanal e sync.
- `src/config.py` → datas relativas (`1y`, `6M`, `180d`) via `resolve_relative_date`;
  **escopo do projeto** (`scope_commodities`/`scope_polos` = café/Sul de Minas +
  Cerrado Mineiro) e `*_lookback_days` para o histórico de 1 ano.
- `src/ingestion/era5.py` → modo **mensal**: `build_month_request`, `ingest_month`,
  `ingest_backfill` e CLI `--month`/`--backfill-days`; layout
  `raw/climate_era5/month=YYYY-MM/`.
- `src/ingestion/chirps.py` → CLI `--lookback-days` para o backfill diário.
- `src/processing/zonal_stats.py` → `era5_zonal_hourly` lê arquivos **diários e
  mensais**, fatiando o NetCDF por dia-alvo.
- `src/analytics/gold.py` → `build_gold_weekly_analytics()`: agregação **W-SUN**
  (semana inicia na segunda-feira), `crop_stress_index = max(-déficit, 0)`,
  `alerta_estresse > 15 mm/semana`; fallback sintético determinístico (seed=42).
- `src/database/models.py` → ORM SQLAlchemy (`DimPolo`, `FactClimaSemanalCafe`,
  `FactCotacoesCafeSaca`) + `create_engine_from_settings()` com **fallback SQLite**
  e `enable_postgis()` (coluna `geom`, SRID 4326).
- `src/database/sync.py` → `sync_weekly_gold_to_db()`/`sync_gold_to_db()`:
  **UPSERT idempotente** (ON CONFLICT) por chave natural, testado em SQLite.
- `app.py` → painel Streamlit: cards, mapa Plotly das cidades do Sul de Minas,
  eixo duplo (CSI × preço) e heatmap de correlação clima × mercado.
- `Dockerfile` + `docker-compose.yml` (Python 3.12-slim, `mem_limit: 800m` p/
  EC2 t3.micro) e `scripts/deploy_aws.sh` (`aws s3 sync` + deploy do container).

### 7.4 Métricas calculadas e convenções (formulário)

#### Financeiro — diário (camada `processed/finance`)
| Métrica | Fórmula | Onde |
| --- | --- | --- |
| Retorno diário | `retorno_diario_pct_t = (P_t / P_{t-1} − 1) × 100` | `finance_transform.add_risk_metrics` |
| Volatilidade 21d diária | Desvio padrão **amostral** (`ddof=1`) dos retornos diários dos **últimos 21 pregões** (~1 mês comercial, janela móvel); exige ≥ `min_periods=5` observações, senão `NaN` | `add_risk_metrics` |
| Volatilidade 21d anualizada | `volatilidade_21d_diaria × √252` (252 dias úteis/ano) | `add_risk_metrics` |

> Onde `P_t` é o `preco_brl_saca` (fechamento convertido por `BRL=X` via `merge_asof`).

#### Financeiro — semanal (camada `gold/analytics_coffee_stress_weekly.parquet`)
| Métrica | Fórmula | Onde |
| --- | --- | --- |
| Preço da semana | **Último** fechamento BRL/saca da semana (W-SUN) | `gold.aggregate_weekly` |
| **Volatilidade 21d semanal** | **Snapshot `'last'`** da `volatilidade_21d_anualizada` na semana (valor de sexta-feira) — **não** é a média das diárias | `gold.aggregate_weekly` |
| Retorno semanal | `retorno_semanal_pct = (P_semana / P_semana_anterior − 1) × 100` | `gold.aggregate_weekly` |
| Volatilidade 4w semanal | `volatilidade_4w_semanal_anualizada = std(retornos semanais, janela 4 semanas, `ddof=1`) × √52` (52 semanas/ano) | `gold._add_weekly_volatility` |

**Interpretação:** a `volatilidade_21d_anualizada` já vem anualizada da camada
diária (`×√252`); a Gold captura o último valor útil de cada semana (sexta-feira).
A `volatilidade_4w_semanal_anualizada` mede a dispersão dos **retornos semanais**
(4 semanas ≈ 1 mês comercial) anualizada por `√52`.

#### Climático — semanal (mesmo arquivo Gold)
| Métrica | Fórmula |
| --- | --- |
| Precipitação semanal | `Σ precipitação_chirps_mm` dos dias da semana |
| ETP semanal | `Σ etp_mm` (ERA5 `pev` ou fallback Hargreaves) |
| Déficit hídrico semanal | `Σ (precipitação − ETP)` por dia |
| Crop Stress Index | `max(−déficit_hidrico_semanal, 0)` — positivo = estresse |
| Alerta de estresse | `crop_stress_index > 15 mm/semana` (limiar configurável) |

## 8. Decisões e Aprendizados Relevantes

- **Escopo café tipo 4/5**: apenas `KC=F`/`ICF=F` + `BRL=X`; polos Sul de Minas e
  Cerrado Mineiro (via `SCOPE_*` no `.env`). O motor continua suportando soja e
  milho se o escopo for ampliado.
- **Backfill de 1 ano**: CHIRPS é diário (365 arquivos ~3 MB); ERA5 usa o modo
  **mensal** (12–13 requisições ao CDS em vez de 365), salvando
  `raw/climate_era5/month=YYYY-MM/` — a camada processed fatia por dia.
- **Datas relativas**: `FINANCE_START_DATE=1y` é resolvido para data literal no
  yfinance via `resolve_relative_date`.
- **tmax/tmin do ERA5-Land**: o catálogo oficial do CDS não possui
  `*_since_previous_post_processing` nesse dataset; a solução é derivar das 24
  horas de `2m_temperature` (padrão agrometeorológico).
- **Particionamento Hive**: valores de partição com `=` (ex.: `ticker=KC=F`)
  quebram o parsing → uso de `ticker_safe` (sanitizado).
- **Sidecars de metadados**: prefixo `_` para não interferirem na descoberta de
  datasets do PyArrow/Athena.
- **Resiliência**: datas sem uma das fontes geram `NaN` + flags `*_disponivel`,
  nunca interrompem o pipeline.
- **Semana no pandas**: `W-MON` ancora a segunda como fim do período (start numa
  terça); `W-SUN` é usado para que `data_semana` comece na segunda-feira.
- **UPSERT por `excluded`**: o `set_` do `ON CONFLICT` deve referenciar
  `excluded.<col>`; `unique=True` + `index=True` criam *índice único* (não
  `UniqueConstraint`) — ambos tratados na detecção da chave natural.
- **Plotly 6**: `scatter_mapbox`/`Scattermapbox` foram renomeados para
  `scatter_map`/`Scattermap`; o app usa fallback para compatibilidade.
- **Conversão BRL**: o `merge_asof` do câmbio deixa `NaN` para datas anteriores
  à primeira cotação do `BRL=X`; o backfill de 1 ano do câmbio resolve isso.

