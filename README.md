# ☕ Hydra — Inteligência Climática e Financeira para o Agronegócio

> Plataforma de monitoramento de **estresse hídrico × preço do café** com arquitetura de
> Data Lake (Medallion), pipeline ETL, camada analítica semanal e dashboard interativo.

![Stack](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Arquitetura](https://img.shields.io/badge/Medallion-Data%20Lake-2ea44f)
![Banco](https://img.shields.io/badge/PostgreSQL%2F%2FSQLite-blue)
![UI](https://img.shields.io/badge/Streamlit-1.62-red)
![Testes](https://img.shields.io/badge/tests-64%20passed-green)

---

## 🎯 O que o projeto faz

Reúne **cotações do café arábica (KC=F, ICE)** com **dados climáticos de satélite e
reanálise** sobre os polos produtores do Brasil para responder:

- ❓ **Chuva abaixo do esperado está correlacionada com a alta do preço do café?**
- 🌧️ **Onde está havendo estresse hídrico agora?** (Crop Stress Index semanal)
- 📈 **Quão volátil está o mercado?** (volatilidade 21d anualizada `×√252`)

### Fontes de dados
| Fonte | Dado | Granularidade |
| --- | --- | --- |
| [Yahoo Finance](https://finance.yahoo.com) | `KC=F` (café arábica ICE) + `BRL=X` | diária |
| [CHIRPS (UCSB)](https://www.chc.ucsb.edu/data/chirps) | precipitação por satélite | diária · 0,05° |
| [ERA5-Land (Copernicus CDS)](https://cds.climate.copernicus.eu) | temperatura, radiação, ETP | horária → diária |

## 🏗️ Arquitetura (resumo)

```
Yahoo · CHIRPS · ERA5 ──► RAW (Medallion Bronze) ──► PROCESSED (Silver)
                                                          │
      Streamlit (:8501)  ◄── GOLD semanal  ◄──────────────┘
      PostgreSQL/SQLite  ◄── sync
```

- **Raw** — particionamento Hive (`dt=YYYY-MM-DD`), compatível com Athena/Glue.
- **Processed** — estatística zonal por polo, balanço hídrico FAO-56 e risco financeiro.
- **Gold** — semanais: `crop_stress_index`, alertas (`> 15 mm/semana`), preço BRL/saca,
  retorno semanal e volatilidade 21d/4w anualizadas.

## 🚀 Quickstart

```powershell
# 1) Ambiente
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-lock.txt

# 2) Bootstrap + configuração
python setup_environment.py --network-check
copy .env.example .env        # preencha CDS_KEY (opcional p/ ERA5)

# 3) Dados + analytics
python scripts/backfill_1y.py --skip-era5
python -m src.database.sync

# 4) Dashboard
streamlit run app.py          # http://localhost:8501
```

> 📖 Manual completo passo a passo: **[MANUAL_DE_USO.md](MANUAL_DE_USO.md)** ·
> 📚 Documentação acadêmica: **[APRESENTACAO_PROJETO.md](APRESENTACAO_PROJETO.md)**

## 📦 Estrutura

```
app.py                     # Dashboard Streamlit (KPIs + gráficos)
src/
  ingestion/               # Yahoo Finance · CHIRPS · ERA5-Land
  processing/              # pipeline · balanço hídrico · zonal stats · risco
  analytics/gold.py        # camada analítica semanal
  database/sync.py         # persistência relacional
  storage/                 # backend local/S3 (Medallion)
scripts/
  backfill_1y.py           # fluxo completo de 1 ano
  deploy_aws.sh            # deploy AWS + Docker
tests/                     # 64 testes (pytest)
MANUAL_DE_USO.md           # guia operacional ponta a ponta
```

## ✅ Qualidade

```powershell
python -m pytest tests -q     # 64 passed
python -m ruff check src scripts app.py tests
python -m mypy src scripts
```

## 🧩 Limitações conhecidas

- `ICF=F` (B3) **não existe** no Yahoo Finance → referência oficial é `KC=F` (ICE).
- ERA5-Land exige **aceite manual da licença** no portal Copernicus; sem ele, use
  `--skip-era5` (fallback ETP Hargreaves).

---
Feito com ❤️ e dados abertos — projeto acadêmico (PUC Minas).
