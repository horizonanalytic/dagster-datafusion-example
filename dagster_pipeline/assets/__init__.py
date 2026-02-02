"""Dagster assets for generating Parquet files"""

from dagster import load_assets_from_modules

from . import healthcare, ecommerce

# Load all assets from submodules
healthcare_assets = load_assets_from_modules([healthcare])
ecommerce_assets = load_assets_from_modules([ecommerce])

# Export all assets
all_assets = [*healthcare_assets, *ecommerce_assets]

__all__ = ["all_assets", "healthcare_assets", "ecommerce_assets"]
