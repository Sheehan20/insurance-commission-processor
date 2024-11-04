"""
Commission Reconciliation Main Process
A complete solution for processing and analyzing insurance commission data
"""

import pandas as pd
import os
from pathlib import Path
from src.parser import CenteneParser, EmblemParser
from src.normalizer import DataNormalizer
from src.analyzer import PerformanceAnalyzer
from src.parser.healthfirst_parser import HealthfirstParser

def parse_carrier_data(parser_class, file_path):
    """Parse data for a single carrier"""
    parser = parser_class(str(file_path))  # Convert Path to string
    print(f"\nParsing {parser.get_carrier_name()} data...")
    
    try:
        # First read Excel file
        parser.read_excel()
        print(f"Excel file read successfully: {file_path}")
        
        # Then parse data
        data = parser._parse_impl()
        
        # Ensure required fields exist
        data['carrier_name'] = parser.get_carrier_name()
        data['commission_period'] = parser.get_commission_period()
        
        # Validate required fields
        assert not data.empty, f"{parser.get_carrier_name()} data cannot be empty"
        assert 'agent_name' in data.columns, f"{parser.get_carrier_name()} data missing agent_name"
        assert 'commission_amount' in data.columns, f"{parser.get_carrier_name()} data missing commission_amount"
        
        # Validate data types
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

def process_data_parsing(all_data):
    """Process data parsing results"""
    print("\nData parsing statistics:")
    total_records = 0
    for i, data in enumerate(all_data):
        print(f"Data source {i+1}: {len(data)} records")
        total_records += len(data)
    print(f"Total records: {total_records}")

def process_data_normalization(all_data):
    """
    Process data normalization
    Including:
    1. Data format standardization
    2. Field name standardization  
    3. Agent name deduplication
    4. Date field standardization
    """
    print("\nStarting data normalization...")
    
    # Merge data
    print("Merging data...")
    combined_data = pd.concat(all_data, ignore_index=True)
    assert not combined_data.empty, "Merged data cannot be empty"
    print(f"Total records after merge: {len(combined_data)}")
    print(f"Current columns: {combined_data.columns.tolist()}")
    
    # Ensure required fields exist
    required_fields = ['carrier_name', 'commission_period', 'agent_name', 'commission_amount']
    missing_fields = set(required_fields) - set(combined_data.columns)
    if missing_fields:
        print(f"Warning: Missing required fields: {missing_fields}")
        for field in missing_fields:
            combined_data[field] = None
    
    # Normalize data
    print("\nExecuting data normalization...")
    normalizer = DataNormalizer(combined_data)
    normalized_data = normalizer.normalize()
    
    # Validate normalized schema
    required_columns = [
        'agent_name', 'commission_amount', 'carrier_name', 
        'commission_period', 'agency_name', 'member_id',
        'enrollment_date', 'disenrollment_date'
    ]
    for col in required_columns:
        assert col in normalized_data.columns, f"Normalized data missing {col} column"
    
    # Validate agent name deduplication
    print("\nValidating name standardization...")
    original_agent_count = combined_data['agent_name'].nunique()
    normalized_agent_count = normalized_data['agent_name'].nunique()
    print(f"Agent count change: {original_agent_count} -> {normalized_agent_count}")
    
    # Validate date standardization
    print("\nValidating date standardization...")
    date_columns = ['enrollment_date', 'disenrollment_date']
    for col in date_columns:
        if col in normalized_data.columns:
            non_null_dates = normalized_data[col].dropna()
            if len(non_null_dates) > 0:
                assert pd.to_datetime(non_null_dates, errors='coerce').notna().any(), \
                    f"{col} has incorrect date format"
            print(f"- {col}: {len(non_null_dates)} valid dates")
    
    # Save CSV file
    print("\nSaving normalized data...")
    output_dir = Path("data/processed")
    output_dir.mkdir(exist_ok=True)
    csv_path = output_dir / "normalized_commissions.csv"
    normalized_data.to_csv(csv_path, index=False)
    
    # Verify CSV file
    assert os.path.exists(csv_path), "CSV file not generated"
    saved_data = pd.read_csv(csv_path)
    assert len(saved_data) == len(normalized_data), "Saved record count mismatch"
    
    print("\nNormalization complete:")
    print(f"- Total records: {len(normalized_data):,}")
    print(f"- Unique agents: {normalized_data['agent_name'].nunique():,}")
    print(f"- Total commission: ${normalized_data['commission_amount'].sum():,.2f}")
    print(f"- Save path: {csv_path}")
    
    return normalized_data

def process_top_performers(normalized_data):
    """Calculate and print top performers"""
    print("\nStarting Top 10 agents calculation...")
    
    analyzer = PerformanceAnalyzer(normalized_data)
    
    # Get period summary
    summary = analyzer.get_period_summary(period="2024-06")
    print("\nJune 2024 Summary:")
    print(f"- Total commission: ${summary['total_commission']:,.2f}")
    print(f"- Total transactions: {summary['total_transactions']:,}")
    print(f"- Unique agents: {summary['unique_agents']:,}")
    print(f"- Carriers: {', '.join(summary['carriers'])}")
    
    # Calculate and verify top performers
    top_performers = analyzer.calculate_top_performers(n=10, period="2024-06")
    
    # Basic validation
    assert len(top_performers) == 10, "Must return 10 agents"
    assert all(top_performers['total_commission'].notna()), "Commission amounts cannot be null"
    assert all(top_performers['agent_name'].notna()), "Agent names cannot be null"
    
    # Sort validation
    commission_values = top_performers['total_commission'].values
    print("\nSort validation:")
    print("Commission value sequence:", commission_values)
    
    for i in range(len(commission_values)-1):
        current = float(commission_values[i])
        next_val = float(commission_values[i+1])
        print(f"Checking position {i}: {current} >= {next_val}")
        assert current >= next_val, f"Position {i} commission({current}) less than position {i+1}({next_val})"
    
    # Print results
    print("\nTop 10 Agents (June 2024):")
    print("-" * 50)
    for i, row in top_performers.iterrows():
        print(f"{i+1}. {row['agent_name']}")
        print(f"   Total Commission: ${row['total_commission']:,.2f}")
        print(f"   Average Commission: ${row['avg_commission']:,.2f}")
        print(f"   Transactions: {row['transaction_count']:,}")
        print(f"   Carriers: {row['carriers']}")
        print("-" * 50)

def process_all_carriers():
    """Process data from all insurance carriers and generate report"""
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        
        data_path = Path("data/raw")
        
        print(f"Current working directory: {os.getcwd()}")
        print(f"Looking for files in: {data_path.absolute()}")
        
        parsers = [
            (CenteneParser, data_path / "Centene 06.2024 Commission.xlsx"),
            (EmblemParser, data_path / "Emblem 06.2024 Commission.xlsx"),
            (HealthfirstParser, data_path / "Healthfirst 06.2024 Commission.xlsx")
        ]

        all_data = []
        for parser_class, file_path in parsers:
            try:
                if not file_path.exists():
                    print(f"Warning: File not found: {file_path}")
                    continue
                    
                print(f"\nProcessing {file_path.name}...")
                data = parse_carrier_data(parser_class, file_path)
                
                print(f"Data sample:")
                print(data.head())
                print(f"Commission total: ${data['commission_amount'].sum():,.2f}")
                
                all_data.append(data)
                print(f"Cumulative parsed records: {sum(len(df) for df in all_data)}")
            except Exception as e:
                print(f"\nError processing {parser_class.__name__}:")
                print(f"- File: {file_path}")
                print(f"- Error: {str(e)}")
                raise
        
        # 1. Process parsing results
        process_data_parsing(all_data)
        
        # 2. Data normalization
        normalized_data = process_data_normalization(all_data)
        
        # 3. Calculate top agents
        process_top_performers(normalized_data)
        
        print("\nAll processing completed successfully!")
        
    except Exception as e:
        print(f"\nError processing data: {str(e)}")
        print("Current completed steps:")
        print("- [√] Data parsing")
        print("- [?] Data normalization")
        print("- [?] Top 10 calculation")
        raise

if __name__ == "__main__":
    process_all_carriers()