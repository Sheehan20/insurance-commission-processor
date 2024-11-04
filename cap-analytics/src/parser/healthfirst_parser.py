"""
Healthfirst specific commission parser
Handles parsing and standardization of Healthfirst commission data files
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any
from pathlib import Path
from src.parser.base_parser import BaseCommissionParser

class HealthfirstParser(BaseCommissionParser):
    """
    Parser for Healthfirst commission data files
    Implements specific data format and business rules for Healthfirst
    """
    
    def __init__(self, file_path: str):
        """
        Initialize parser with file path and setup caching
        
        Args:
            file_path: Path to input Excel file
        """
        super().__init__(file_path)
        self._carrier_name = "Healthfirst"
        self._commission_period = None  # Cache commission period
    
    def get_carrier_name(self) -> str:
        """Get carrier name identifier"""
        return self._carrier_name
    
    def get_commission_period(self) -> str:
        """
        Extract commission period from filename using caching
        
        Returns:
            str: Commission period in 'YYYY-MM' format
            
        Uses caching to avoid repeated filename parsing
        """
        if self._commission_period is None:
            try:
                # Parse once and cache result
                filename = Path(self.file_path).name
                period_part = filename.split(' ')[1]  
                month, year = period_part.split('.')
                self._commission_period = f"{year}-{month.zfill(2)}"
            except Exception as e:
                print(f"Warning: Unable to parse period from filename: {e}")
                self._commission_period = "2024-06"  # Default value
                
        return self._commission_period
    
    def _clean_commission_amount(self, amount: Any) -> float:
        """
        Clean and standardize commission amounts
        
        Args:
            amount: Raw commission amount value
            
        Returns:
            float: Standardized amount value, 0.0 if invalid
            
        Handles:
        - Missing/null values
        - String formats with currency symbols
        - Negative amounts in parens or with minus
        - Invalid/non-numeric values
        """
        try:
            if pd.isna(amount):
                return 0.0
            
            if isinstance(amount, (int, float)):
                return float(amount)
            
            if isinstance(amount, str):
                # Remove currency formatting
                amount = amount.replace('$', '').replace(',', '').strip()
                
                # Handle negative values 
                if amount.startswith('(') and amount.endswith(')'):
                    amount = '-' + amount[1:-1]
                if amount.startswith('-$'): 
                    amount = '-' + amount[2:]
                    
                # Keep only valid numeric characters
                cleaned = ''.join(c for c in amount if c.isdigit() or c in '.-')
                return float(cleaned or 0)
                
        except Exception as e:
            print(f"Warning: Invalid amount format '{amount}': {e}")
            
        return 0.0

    def _clean_agent_name(self, name: Any) -> str:
        """
        Clean and standardize agent names
        
        Args:
            name: Raw agent name value
            
        Returns:
            str: Cleaned agent name or None if invalid
            
        Steps:
        1. Handle missing/invalid values
        2. Strip whitespace
        3. Standardize capitalization
        """
        if pd.isna(name) or not str(name).strip():
            return None  
            
        name = str(name).strip()
        name = " ".join(word.capitalize() for word in name.split())
        return name

    def _parse_impl(self) -> pd.DataFrame:
        """
        Implementation of Healthfirst-specific parsing logic
        
        Returns:
            pd.DataFrame: Parsed and standardized commission data
            
        Process:
        1. Copy raw data
        2. Print debug info
        3. Process records with error handling
        4. Standardize fields and formats  
        5. Validate output
        """
        print(f"\nStarting to parse {self.get_carrier_name()} data...")
        
        # Print debug info
        print("\nRaw data columns:", self.raw_data.columns.tolist())
        print("\nRaw data sample:")
        print(self.raw_data.head())
        
        try:
            df = self.raw_data.copy()
            standardized_data = []
            
            # Process each record
            for index, row in df.iterrows():
                try:
                    # Extract agent name with fallback to agency
                    agent_name = self._clean_agent_name(row.get('Producer Name'))
                    if not agent_name:
                        agency_name = self._clean_agent_name(row.get('Producer Type'))
                        agent_name = agency_name if agency_name else "Unknown Agent"
                    
                    # Map Healthfirst-specific fields to standard format
                    record = {
                        'carrier_name': self.get_carrier_name(),
                        'commission_period': self.get_commission_period(),
                        'agent_name': agent_name,
                        'agency_name': '', 
                        'member_id': str(row.get('Member ID', '')).strip(),
                        'member_name': str(row.get('Member Name', '')).strip(),
                        'plan_name': str(row.get('Product', '')).strip(),
                        'enrollment_date': self.standardize_date(row.get('Member Effective Date')),
                        'disenrollment_date': self.standardize_date(row.get('Disenrolled Date')),
                        'commission_amount': self._clean_commission_amount(row.get('Amount')),
                        'transaction_type': str(row.get('Enrollment Type', '')).strip(),
                        'policy_number': '',
                        'effective_date': self.standardize_date(row.get('Member Effective Date')),
                        'processed_date': None
                    }
                    
                    standardized_data.append(record)
                    
                    # Progress logging
                    if (index + 1) % 1000 == 0:
                        print(f"Processed {index + 1} records...")
                    
                except Exception as e:
                    print(f"Error processing row {index + 2}: {str(e)}")
                    continue
            
            if not standardized_data:
                raise Exception(f"No data could be parsed from {self.file_path}")
                
            # Create DataFrame
            result = pd.DataFrame(standardized_data)
            
            # Print summary stats
            print(f"\n{self.get_carrier_name()} parsing complete:")
            print(f"- Total records: {len(result):,}")
            print(f"- Total commission: ${result['commission_amount'].sum():,.2f}")
            print(f"- Unique agents: {result['agent_name'].nunique():,}")
            
            # Validate required fields
            required_fields = ['carrier_name', 'commission_period', 'agent_name', 'commission_amount']
            missing_fields = [field for field in required_fields if field not in result.columns]
            if missing_fields:
                raise Exception(f"Missing required fields: {missing_fields}")
            
            return result
            
        except Exception as e:
            print(f"\nError parsing {self.get_carrier_name()} data: {str(e)}")
            raise