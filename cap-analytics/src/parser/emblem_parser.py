"""
Emblem specific commission parser
Handles parsing and standardization of Emblem commission data files
"""

from .base_parser import BaseCommissionParser
import pandas as pd 
import numpy as np
from datetime import datetime

class EmblemParser(BaseCommissionParser):
    """
    Parser for Emblem commission data files
    Handles the specific data format and business rules used by Emblem
    """
    
    def __init__(self, file_path: str):
        """
        Initialize Emblem parser
        
        Args:
            file_path: Path to the input Excel file containing Emblem data
        """
        super().__init__(file_path)
        self.carrier_name = "Emblem"
    
    def get_carrier_name(self) -> str:
        """Get carrier name identifier"""
        return self.carrier_name
    
    def get_commission_period(self) -> str:
        """
        Extract commission period from filename or data
        Expected filename format: 'Emblem MM.YYYY Commission.xlsx'
        
        Returns:
            str: Commission period in 'YYYY-MM' format
            
        Example:
            'Emblem 06.2024 Commission.xlsx' -> '2024-06'
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
            amount: Raw commission amount value
            
        Returns:
            float: Cleaned commission amount, 0.0 if invalid
            
        Handles:
        - Missing/null values
        - String format with currency symbols
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
            # Handle negative values in parentheses
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
        Implementation of Emblem-specific parsing logic
        
        Returns:
            pd.DataFrame: Parsed and standardized commission data
            
        Key steps:
        1. Copy raw data 
        2. Extract fields using Emblem's column names
        3. Standardize data formats
        4. Handle missing/invalid data
        5. Return standardized DataFrame
        """
        df = self.raw_data.copy()
        standardized_data = []
        
        for index, row in df.iterrows():
            try:
                # Combine first and last name
                member_name = " ".join(filter(None, [
                    str(row.get('Member First Name', '')).strip(),
                    str(row.get('Member Last Name', '')).strip()
                ]))
                
                # Map Emblem-specific columns to standard format
                record = {
                    'carrier_name': self.get_carrier_name(),
                    'commission_period': self.get_commission_period(),
                    'agent_name': str(row.get('Rep Name', '')).strip(),
                    'agency_name': str(row.get('Payee Name', '')).strip(),
                    'member_id': str(row.get('Member ID', '')).strip(),
                    'member_name': member_name,
                    'plan_name': str(row.get('Plan', '')).strip(),
                    'enrollment_date': self.standardize_date(row.get('Effective Date')), 
                    'disenrollment_date': self.standardize_date(row.get('Term Date')),
                    'commission_amount': self._clean_commission_amount(row.get('Payment')),
                    'transaction_type': 'Commission', # Default value for Emblem
                    'policy_number': str(row.get('Member HIC', '')).strip(),
                    'effective_date': self.standardize_date(row.get('Effective Date')),
                    'processed_date': None
                }
                standardized_data.append(record)
                
            except Exception as e:
                print(f"Error processing row {index + 2}: {str(e)}")
                continue
            
        result = pd.DataFrame(standardized_data)
        
        # Print summary statistics
        print(f"\n{self.get_carrier_name()} parsing complete:")
        print(f"- Total records: {len(result):,}")
        print(f"- Total commission: ${result['commission_amount'].sum():,.2f}")
        print(f"- Unique agents: {result['agent_name'].nunique():,}")
        
        return result