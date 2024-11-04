# Insurance Commission Data Reconciliation

## Project Overview

This project provides a solution for processing and analyzing insurance commission data for Field Marketing Organizations (FMOs) and agencies. It automates commission data processing from multiple carriers, normalizes the data into a standardized format, and generates performance analytics reports.

### Key Features

- Multi-carrier commission data parsing
- Data normalization and deduplication
- Agent name standardization
- Performance analytics and reporting
- CSV data export for database integration

## Technical Architecture

### Core Components

1. **Parsers** (`src/parser/`)

   - `base_parser.py`: Abstract base class defining common parser interface
   - `centene_parser.py`: Centene-specific data parser
   - `emblem_parser.py`: Emblem-specific data parser
   - `healthfirst_parser.py`: Healthfirst-specific data parser

2. **Data Processing** (`src/`)

   - `normalizer.py`: Data cleaning and standardization
   - `performance_analyzer.py`: Commission analytics and reporting

3. **Tests** (`tests/`)
   - Comprehensive test suite for parsing, normalization, and analytics

## Installation & Setup

### Prerequisites

- Python 3.8+
- pandas
- pytest
- openpyxl (for Excel file handling)

## Usage

### Running the Data Processing Pipeline

```python
# Run all tests
python cap-analytics/main.py
```

### Expected Outputs

1. Normalized commission data CSV file (`data/processed/normalized_commissions.csv`)
2. Top 10 agents performance report
3. Processing statistics and validation results

### Data Schema

The normalized data follows this schema:

- carrier_name: Insurance carrier identifier
- commission_period: YYYY-MM format
- agent_name: Standardized agent name
- agency_name: Agency name if available
- member_id: Unique member identifier
- member_name: Member full name
- plan_name: Insurance plan name
- enrollment_date: Plan enrollment date
- disenrollment_date: Plan termination date
- commission_amount: Commission amount in USD
- transaction_type: Type of commission transaction
- policy_number: Policy identifier
- effective_date: Commission effective date
- processed_date: Processing date

## Features

### 1. Multi-Carrier Data Parsing

- Supports multiple carrier-specific formats
- Extensible parser architecture
- Robust error handling and validation

### 2. Data Normalization

- Standardized field names and formats
- Agent name deduplication
- Date format standardization
- Amount format cleaning

### 3. Analytics

- Top performer calculations
- Commission summaries by carrier
- Agent performance metrics

## Testing

```bash

# Run specific test category

# Test data parsing
pytest tests/test_assignment.py -v -k test_data_parsing

# Test data normalization
pytest tests/test_assignment.py -v -k test_data_normalization

# Test Top 10 calculation
pytest tests/test_assignment.py -v -k test_top_performers

# Test overall functionality
pytest tests/test_assignment.py -v -k test_deliverables
```

## Extensibility

The system is designed for easy extension:

1. Add new carrier support by creating a new parser class
2. Extend normalization rules in DataNormalizer
3. Add new analytics in PerformanceAnalyzer
4. Modify schema by updating BaseCommissionParser
