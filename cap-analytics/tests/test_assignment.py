"""
Delta Care Commission Reconciliation Tests

Testing three core requirements:
1. Excel data file parsing
2. Data normalization and deduplication
3. Top 10 agents calculation and report generation

This module provides comprehensive testing for commission data processing pipeline:
- File parsing validation
- Data format standardization
- Commission calculations
- Report generation
"""

import pytest
import pandas as pd
import os
from pathlib import Path
from src.parser import CenteneParser, EmblemParser, HealthfirstParser
from src.normalizer import DataNormalizer
from src.analyzer import PerformanceAnalyzer

@pytest.fixture
def data_dir():
    """
    Get data file directory fixture
    
    Returns:
        Path: Directory containing raw commission data files
    """
    return Path("data/raw")

def parse_carrier_data(parser_class, file_path):
    """
    Parse data for a single carrier with validation
    
    Args:
        parser_class: Parser class to use
        file_path: Path to carrier's Excel file
        
    Returns:
        pd.DataFrame: Parsed and validated data
        
    Raises:
        Exception: If parsing or validation fails
        
    Validations:
    - Data not empty
    - Required columns present
    - Commission amounts numeric
    """
    parser = parser_class(file_path)
    print(f"\nParsing {parser.get_carrier_name()} data...")
    
    try:
        parser.read_excel()
        print(f"Excel file read successfully: {file_path}")
        
        data = parser._parse_impl()
        
        # Add required fields
        data['carrier_name'] = parser.get_carrier_name()
        data['commission_period'] = parser.get_commission_period()
        
        # Validate data
        assert not data.empty, f"{parser.get_carrier_name()} data cannot be empty"
        assert 'agent_name' in data.columns, f"{parser.get_carrier_name()} data missing agent_name"
        assert 'commission_amount' in data.columns, f"{parser.get_carrier_name()} data missing commission_amount"
        assert pd.to_numeric(data['commission_amount'], errors='coerce').notna().all(), \
            f"{parser.get_carrier_name()} commission amounts contain non-numeric data"
        
        print(f"- Successfully parsed {len(data)} records")
        print(f"- Current columns: {data.columns.tolist()}")
        return data
        
    except Exception as e:
        print(f"Parsing error ({parser.get_carrier_name()}): {str(e)}")
        print(f"File path: {file_path}")
        if hasattr(parser, 'raw_data') and parser.raw_data is not None:
            print(f"Raw data shape: {parser.raw_data.shape}")
            print(f"Raw data columns: {parser.raw_data.columns.tolist()}")
        raise

@pytest.fixture
def parsed_data(data_dir):
    """
    Fixture to parse all Excel data files
    
    Args:
        data_dir: Directory containing commission files
        
    Returns:
        list: List of parsed DataFrames
        
    Handles:
    - Multiple carrier files 
    - Error tracking
    - Progress reporting
    """
    parsers = [
        (CenteneParser, data_dir / "Centene 06.2024 Commission.xlsx"),
        (EmblemParser, data_dir / "Emblem 06.2024 Commission.xlsx"),
        (HealthfirstParser, data_dir / "Healthfirst 06.2024 Commission.xlsx")
    ]
    
    all_data = []
    for parser_class, file_path in parsers:
        try:
            data = parse_carrier_data(parser_class, file_path)
            all_data.append(data)
            print(f"Cumulative parsed records: {sum(len(df) for df in all_data)}")
        except Exception as e:
            print(f"\nError processing {parser_class.__name__}:")
            print(f"- File: {file_path}")
            print(f"- Error: {str(e)}")
            raise
    
    return all_data

def get_normalized_data(parsed_data):
    """
    Normalize and combine parsed data
    
    Args:
        parsed_data: List of parsed DataFrames
        
    Returns:
        pd.DataFrame: Combined and normalized data
    """
    combined_data = pd.concat(parsed_data, ignore_index=True)
    normalizer = DataNormalizer(combined_data)
    return normalizer.normalize()

def generate_top_performers_report(normalized_data):
    """
    Generate top 10 agents performance report
    
    Args:
        normalized_data: Normalized commission data
        
    Outputs:
    - Period summary statistics
    - Top 10 agents by commission
    - Performance details per agent
    - Carrier distribution
    """
    print("\n" + "="*80)
    print("                     TOP 10 COMMISSION EARNERS - JUNE 2024")
    print("="*80)
    
    analyzer = PerformanceAnalyzer(normalized_data)
    summary = analyzer.get_period_summary(period="2024-06")
    top_performers = analyzer.calculate_top_performers(n=10, period="2024-06")
    
    # Print summary information
    print(f"\nPeriod Summary:")
    print(f"- Total Commission: ${summary['total_commission']:,.2f}")
    print(f"- Total Transactions: {summary['total_transactions']:,}")
    print(f"- Total Agents: {summary['unique_agents']:,}")
    print(f"- Insurance Carriers: {', '.join(summary['carriers'])}")
    
    # Print header
    print("\n" + "-"*80)
    print(f"{'Rank':4} {'Agent Name':30} {'Total Commission':>15} {'Avg Commission':>15} {'Transactions':>12}")
    print("-"*80)
    
    # Print detailed agent information
    for i, row in top_performers.iterrows():
        rank = f"{i+1}."
        name = row['agent_name'] if row['agent_name'] else "N/A"
        total = f"${row['total_commission']:,.2f}"
        avg = f"${row['avg_commission']:,.2f}"
        trans = f"{row['transaction_count']:,}"
        
        print(f"{rank:4} {name:30} {total:>15} {avg:>15} {trans:>12}")
        # Print carrier information
        if row['carriers'] != 'N/A':
            print(f"     Carriers: {row['carriers']}")
    
    print("-"*80)
    print(f"\nReport Generated: {summary['generated_at']}")
    print("="*80)

def test_data_parsing(parsed_data):
    """
    Test data parsing functionality
    
    Args:
        parsed_data: List of parsed DataFrames
        
    Validates:
    - Number of data sources
    - Data types
    - Record counts
    """
    assert len(parsed_data) == 3, "Should have 3 data sources"
    for data in parsed_data:
        assert isinstance(data, pd.DataFrame), "Parse result should be DataFrame"
    
    print("\nData parsing statistics:")
    total_records = 0
    for i, data in enumerate(parsed_data):
        print(f"Data source {i+1}: {len(data)} records")
        total_records += len(data)
    print(f"Total records: {total_records}")

def test_data_normalization(parsed_data):
    """
    Test data normalization process
    
    Args:
        parsed_data: List of parsed DataFrames
        
    Outputs:
    - Normalized CSV file
    - Processing statistics
    """
    print("\nExecuting data normalization...")
    normalized_data = get_normalized_data(parsed_data)
    
    output_dir = Path("data/processed")
    output_dir.mkdir(exist_ok=True)
    csv_path = output_dir / "normalized_commissions.csv"
    normalized_data.to_csv(csv_path, index=False)
    
    print("\nNormalization complete:")
    print(f"- Total records: {len(normalized_data):,}")
    print(f"- Unique agents: {normalized_data['agent_name'].nunique():,}")
    print(f"- Total commission: ${normalized_data['commission_amount'].sum():,.2f}")
    print(f"- Output file: {csv_path}")

def test_top_performers(parsed_data):
    """
    Test top performers report generation
    
    Args:
        parsed_data: List of parsed DataFrames
    """
    normalized_data = get_normalized_data(parsed_data)
    generate_top_performers_report(normalized_data)

def test_deliverables(parsed_data):
    """
    Execute all deliverables in sequence
    
    Args:
        parsed_data: List of parsed DataFrames
        
    Tests:
    1. Data normalization
    2. Top performers report
    
    Handles:
    - Error tracking
    - Progress reporting
    - Status indication
    """
    try:
        print("\nCOMMISSION DATA ANALYSIS")
        print("="*50)
        
        print("\n1. Normalizing Commission Data...")
        test_data_normalization(parsed_data)
        
        print("\n2. Generating Top Performers Report...")
        test_top_performers(parsed_data)
        
        print("\nAll deliverables completed successfully!")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("Current completed steps:")
        print("- [√] Data parsing")
        print("- [?] Data normalization")
        print("- [?] Top 10 calculation")
        raise

if __name__ == "__main__":
    pytest.main([__file__, '-v'])