# BCP Data Warehouse

A comprehensive data warehouse project for BC Playthings, consolidating historical data from three different Point of Sale systems.

## Project Overview

BC Playthings has used three different POS systems over its history:
1. **AmberPOS** (earliest) - Data available in Excel format
2. **Lightspeed X-Series** (2022-2024) - Data extracted via API
3. **Odoo** (current) - Active system with ongoing data

This project creates a unified data warehouse that allows historical analysis across all three systems.

## Directory Structure

```
BCP-data-warehouse/
├── amber/          # AmberPOS data analysis and extraction
├── lightspeed/     # Lightspeed X-Series data extraction (completed)
├── odoo/          # Odoo data analysis and extraction
├── warehouse/     # Unified data warehouse schema and ETL
└── analysis/      # Cross-system analysis and reporting
```

## Project Status

### ✅ Lightspeed X-Series (COMPLETED)
- **37,057 records exported** across 17 endpoints
- **Audit verified** - 100% data consistency confirmed
- **Complete business data**: Products (8,301), Customers (2,265), Sales (19,944)
- **Export format**: JSONL files with CSV conversion available

### 🔄 AmberPOS (NEXT)
- Excel data analysis and standardization
- Schema mapping to align with Lightspeed structure

### 🔄 Odoo (PENDING)
- Current system data extraction
- Real-time integration planning

### 🔄 Data Warehouse (FUTURE)
- Unified schema design across all three systems
- ETL processes for data consolidation
- Historical timeline reconciliation
- Cross-system analytics capability

## Key Business Data

The most critical data for historical analysis:
- **Sales transactions** (revenue analysis across all systems)
- **Product catalog** (SKU evolution and product lifecycle)
- **Customer data** (customer journey across systems)
- **Inventory movements** (operational history)

## Technology Stack

- **Data Extraction**: Python scripts with API integration
- **Data Format**: JSONL for flexibility, CSV for compatibility
- **Data Storage**: TBD (PostgreSQL, SQLite, or cloud solution)
- **Version Control**: Git for all scripts and schema definitions

## Getting Started

Each subdirectory contains specific instructions for that POS system. Start with the system you need to analyze:

- `lightspeed/README.md` - Lightspeed data extraction (complete)
- `amber/README.md` - AmberPOS data analysis (upcoming)
- `odoo/README.md` - Odoo data extraction (upcoming)

## Timeline

This project represents BC Playthings' complete POS history from the earliest AmberPOS records through the current Odoo implementation, enabling comprehensive business intelligence and historical trend analysis.