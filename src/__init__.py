"""Pacote raiz da plataforma de Inteligencia Climatica e Financeira (Agro).

Modulos:
    config      -> configuracao tipada carregada do .env + logging estruturado
    storage     -> abstracao de armazenamento (local hoje, AWS S3 amanha)
    ingestion   -> ingestores das fontes externas (finance, chirps, era5)
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__: str = "0.2.0"
