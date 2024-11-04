"""
Data normalization class, providing data cleaning and format standardization functionality
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime

class DataNormalizer:
    """Process and normalize commission data"""
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize data normalizer
        
        Args:
            df: Input DataFrame
        """
        self.data = df.copy()
        self.normalized_data = None
        print(f"Initializing normalizer, input data shape: {self.data.shape}")
        print(f"Input data columns: {self.data.columns.tolist()}")
        
        print("\nInput data sample:")
        print(self.data[['agent_name', 'commission_amount']].head())
    
    def normalize_dates(self) -> None:
        """Standardize date formats"""
        date_columns = [
            'enrollment_date',
            'disenrollment_date',
            'effective_date',
            'processed_date'
        ]
        
        for col in date_columns:
            if col in self.data.columns:
                try:
                    self.data[col] = pd.to_datetime(
                        self.data[col], 
                        format='mixed',
                        errors='coerce'
                    ).dt.strftime('%Y-%m-%d')
                    print(f"Normalized date column {col} complete")
                    print(f"Sample {col} values:", self.data[col].head())
                except Exception as e:
                    print(f"Warning: Error normalizing dates in column {col}: {str(e)}")
                    self.data[col] = None

    def normalize_amounts(self) -> None:
        """Standardize amount formats"""
        print("Starting commission amount normalization...")
        
        print("Original amount sample:", self.data['commission_amount'].head())
        
        def clean_amount(val):
            if pd.isna(val):
                return 0.0
                
            try:
                if isinstance(val, (int, float)):
                    return float(val)
                    
                if isinstance(val, str):
                    val = val.replace('$', '').replace(',', '').strip()
                    if val.startswith('(') and val.endswith(')'):
                        val = '-' + val[1:-1]
                    if val.startswith('-$'):
                        val = '-' + val[2:]
                    return float(val)
                    
            except (ValueError, TypeError) as e:
                print(f"Warning: Error converting amount '{val}': {e}")
                return 0.0
                
            return 0.0
        
        self.data['commission_amount'] = (
            self.data['commission_amount']
            .apply(clean_amount)
            .astype(float)
        )
        
        print("Normalized amount sample:", self.data['commission_amount'].head())
        print(f"Commission amount normalization complete, total amount: {self.data['commission_amount'].sum():,.2f}")

    def normalize_names(self) -> None:
        """Standardize agent and agency names"""
        def clean_name(name):
            """Clean and normalize names"""
            if pd.isna(name) or not str(name).strip():
                return "Unknown Agent"
            
            name = str(name).strip()
            name = " ".join(word.capitalize() for word in name.split())
            return name
        
        print("\nNormalizing agent names...")
        if 'agent_name' in self.data.columns:
            original_count = self.data['agent_name'].nunique()
            print("Original agent names sample:", self.data['agent_name'].head())
            
            self.data['agent_name'] = self.data['agent_name'].apply(clean_name)
            
            new_count = self.data['agent_name'].nunique()
            print("Normalized agent names sample:", self.data['agent_name'].head())
            print(f"Agent count: {original_count} -> {new_count}")
        
        print("\nNormalizing agency names...")
        if 'agency_name' in self.data.columns:
            original_count = self.data['agency_name'].nunique()
            self.data['agency_name'] = self.data['agency_name'].apply(clean_name)
            new_count = self.data['agency_name'].nunique()
            print(f"Agency count: {original_count} -> {new_count}")

    def standardize_transaction_types(self) -> None:
        """Standardize transaction types"""
        transaction_mapping = {
            'new': 'New',
            'renewal': 'Renewal',
            'renew': 'Renewal',
            'cancel': 'Cancellation',
            'cancellation': 'Cancellation',
            'terminate': 'Termination',
            'termination': 'Termination',
            'adjustment': 'Adjustment',
            'adj': 'Adjustment',
            'bonus': 'Bonus',
            'commission': 'Commission',
            'reversal': 'Reversal',
            'correction': 'Correction'
        }
        
        if 'transaction_type' in self.data.columns:
            print("\nStandardizing transaction types...")
            original_types = self.data['transaction_type'].unique()
            self.data['transaction_type'] = (
                self.data['transaction_type']
                .str.lower()
                .map(transaction_mapping)
                .fillna('Other')
            )
            new_types = self.data['transaction_type'].unique()
            print(f"Transaction types: {len(original_types)} -> {len(new_types)}")
            print(f"Standardized types: {sorted(new_types)}")

    def clean_member_ids(self) -> None:
        """Clean member IDs"""
        if 'member_id' in self.data.columns:
            print("\nCleaning member IDs...")
            original_count = self.data['member_id'].nunique()
            self.data['member_id'] = (
                self.data['member_id']
                .astype(str)
                .str.strip()
                .str.replace(r'[^a-zA-Z0-9]', '', regex=True)
            )
            new_count = self.data['member_id'].nunique()
            print(f"Member ID count: {original_count} -> {new_count}")

    def validate_required_fields(self) -> None:
        """Validate required fields existence and validity"""
        print("\nValidating required fields...")
        required_fields = {
            'carrier_name',
            'commission_period',
            'agent_name',
            'commission_amount'
        }
        
        # Check field existence
        available_columns = set(self.data.columns)
        print(f"Available columns: {sorted(list(available_columns))}")
        
        missing_fields = required_fields - available_columns
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate amounts
        invalid_amounts = self.data['commission_amount'].isna().sum()
        if invalid_amounts > 0:
            print(f"Warning: {invalid_amounts} rows have invalid commission amounts")
            # Set invalid amounts to 0
            self.data.loc[self.data['commission_amount'].isna(), 'commission_amount'] = 0.0
            print("Invalid amounts set to 0")

    def normalize(self) -> pd.DataFrame:
        """
        Execute all normalization steps
        
        Returns:
            pd.DataFrame: Normalized data
        """
        try:
            print("\nStarting data normalization process...")
            print(f"Initial columns: {self.data.columns.tolist()}")
            
            # Basic normalization
            self.normalize_dates()
            self.normalize_amounts()
            self.normalize_names()
            self.standardize_transaction_types()
            self.clean_member_ids()
            
            # Validate required fields
            self.validate_required_fields()
            
            self.normalized_data = self.data
            
            print("\nNormalization summary:")
            print(f"- Total records: {len(self.normalized_data):,}")
            print(f"- Total commission: ${self.normalized_data['commission_amount'].sum():,.2f}")
            print(f"- Unique agents: {self.normalized_data['agent_name'].nunique():,}")
            print(f"- Unique carriers: {self.normalized_data['carrier_name'].nunique()}")
            
            return self.normalized_data
            
        except Exception as e:
            print(f"\nNormalization error, current columns: {self.data.columns.tolist()}")
            raise Exception(f"Error during normalization: {str(e)}")

    @staticmethod
    def get_standard_columns() -> List[str]:
        """
        Return list of standardized column names
        
        Returns:
            List[str]: Standard column names
        """
        return [
            'carrier_name',
            'commission_period',
            'agent_name',
            'agency_name',
            'member_id',
            'member_name',
            'plan_name',
            'enrollment_date',
            'disenrollment_date',
            'commission_amount',
            'transaction_type',
            'policy_number',
            'effective_date',
            'processed_date'
        ]