"""
Centene specific commission parser
This module handles parsing and standardization of Centene commission data files
"""

from .base_parser import BaseCommissionParser
import pandas as pd
import numpy as np
from datetime import datetime

class CenteneParser(BaseCommissionParser):
    """Parser for Centene commission data files. Handles the specific data format and rules used by Centene."""
    
    def __init__(self, file_path: str):
        """
        Initialize Centene parser
        
        Args:
            file_path: Path to the input Excel file containing Centene commission data
        """
        super().__init__(file_path)
        self.carrier_name = "Centene"
    
    def get_carrier_name(self) -> str:
        """Get carrier name identifier"""
        return self.carrier_name
    
    def get_commission_period(self) -> str:
        """
        Extract commission period from filename or data
        Expected filename format: 'Centene MM.YYYY Commission.xlsx'
        
        Returns:
            str: Commission period in 'YYYY-MM' format
        
        Example:
            'Centene 06.2024 Commission.xlsx' -> '2024-06'
        """
        try:
            print(f"Parsing filename: {self.file_path}")
            filename_parts = self.file_path.split('/')[-1].split(' ')
            month_year = filename_parts[1]  # Extract '06.2024' part
            month, year = month_year.split('.')
            return f"{year}-{month.zfill(2)}" # Ensure month is 2 digits
        except Exception as e:
            print(f"Warning: Unable to parse period from filename: {e}")
            return "2024-06"  # Default fallback value
    
    def _clean_commission_amount(self, amount) -> float:
        """
        Clean and standardize commission amount values
        
        Args:
            amount: Raw commission amount (can be string, int, float or None)
            
        Returns:
            float: Cleaned commission amount, 0.0 if invalid
            
        Handles:
        - Missing/null values
        - String formatting with currency symbols ($)
        - Negative amounts in parentheses
        - Comma-separated numbers
        """
        if pd.isna(amount):
            return 0.0
        
        if isinstance(amount, (int, float)):
            return float(amount)
        
        if isinstance(amount, str):
            print(f"Processing amount: {amount}")
            # Remove currency formatting
            amount = amount.replace('$', '').replace(',', '').strip()
            # Handle negative amounts in parentheses: (100) -> -100
            if amount.startswith('(') and amount.endswith(')'):
                amount = '-' + amount[1:-1]
            try:
                return float(amount)
            except ValueError as e:
                print(f"Warning: Invalid amount format '{amount}': {e}")
                return 0.0
                
        return 0.0
    
    def _parse_impl(self) -> pd.DataFrame:
        """
        Implementation of Centene-specific parsing logic
        
        Returns:
            pd.DataFrame: Parsed and standardized commission data
            
        Key steps:
        1. Copy raw data
        2. Extract fields using Centene's column names
        3. Standardize data formats
        4. Handle missing/invalid data
        5. Return standardized DataFrame
        """
        df = self.raw_data.copy()
        standardized_data = []
        
        for index, row in df.iterrows():
            try:
                # Map Centene-specific column names to standard format
                record = {
                    'carrier_name': self.get_carrier_name(),
                    'commission_period': self.get_commission_period(),
                    'agent_name': str(row.get('Writing Broker Name', '')).strip(),
                    'agency_name': str(row.get('Delta Care CORPORATION', '')).strip(),
                    'member_id': str(row.get('Medicare Beneficiary Identifier (MBI)', '')).strip(),
                    'member_name': str(row.get('Member Name', '')).strip(),
                    'plan_name': str(row.get('Plan Plan Type', '')).strip(),
                    'enrollment_date': self.standardize_date(row.get('Effective Date')),
                    'disenrollment_date': None,
                    'commission_amount': self._clean_commission_amount(row.get('Payment Amount')),
                    'transaction_type': str(row.get('Payment Type', '')).strip(),
                    'policy_number': str(row.get('Policy State', '')).strip(),
                    'effective_date': self.standardize_date(row.get('Effective Date')),
                    'processed_date': None
                }
                standardized_data.append(record)
                
            except Exception as e:
                print(f"Error processing row {index + 2}: {str(e)}")
                continue
            
        result = pd.DataFrame(standardized_data)
        return result