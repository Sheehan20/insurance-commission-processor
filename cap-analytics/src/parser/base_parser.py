"""
Base commission parser class
Defines core interfaces and methods that all carrier-specific parsers must implement
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime

class BaseCommissionParser(ABC):
    """
    Abstract base class defining common interface for all carrier parsers
    
    Key responsibilities:
    - Define required methods for all parsers
    - Provide shared utilities for data processing 
    - Ensure consistent output format
    """
    
    def __init__(self, file_path: str):
        """
        Initialize parser with data file path
        
        Args:
            file_path: Path to input Excel file
        """
        self.file_path = file_path
        self.raw_data = None  # Will hold raw DataFrame
    
    def read_excel(self) -> None:
        """
        Read Excel file into raw_data attribute
        
        Raises:
            Exception: If file cannot be read
        """
        try:
            self.raw_data = pd.read_excel(self.file_path)
            print(f"Successfully read Excel file: {self.file_path}")
            print(f"Raw data columns: {self.raw_data.columns.tolist()}")
        except Exception as e:
            raise Exception(f"Error reading Excel file {self.file_path}: {str(e)}")
    
    @abstractmethod
    def get_carrier_name(self) -> str:
        """
        Get carrier name identifier
        Must be implemented by subclasses
        
        Returns:
            str: Carrier name
        """
        pass
    
    @abstractmethod  
    def get_commission_period(self) -> str:
        """
        Get commission period
        Must be implemented by subclasses
        
        Returns:
            str: Commission period in 'YYYY-MM' format
        """
        pass
    
    @abstractmethod
    def _parse_impl(self) -> pd.DataFrame:
        """
        Carrier-specific parsing implementation
        Must be implemented by subclasses
        
        Returns:
            pd.DataFrame: Parsed data in standardized format
        """
        pass
    
    def parse(self) -> pd.DataFrame:
        """
        Main parsing process implementation
        
        Returns:
            pd.DataFrame: Parsed and validated data
            
        Process:
        1. Read input file
        2. Execute carrier-specific parsing
        3. Add required fields
        4. Validate output
        """
        try:
            print(f"\nStarting to parse {self.get_carrier_name()} data...")
            self.read_excel()
            
            # Execute parsing
            df = self._parse_impl()
            print(f"Initial parsing complete, data shape: {df.shape}")
            
            # Add required fields
            df['carrier_name'] = self.get_carrier_name()
            df['commission_period'] = self.get_commission_period()
            
            print("Adding basic fields:")
            print(f"- carrier_name: {self.get_carrier_name()}")
            print(f"- commission_period: {self.get_commission_period()}")
            
            # Validate data
            self.validate_data(df)
            print(f"Data validation passed, final columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            print(f"Parsing error: {str(e)}")
            raise Exception(f"Error parsing file {self.file_path}: {str(e)}")
    
    def standardize_date(self, date_value: Any) -> str:
        """
        Standardize date format to YYYY-MM-DD
        
        Args:
            date_value: Input date in various formats
            
        Returns:
            str: Standardized date string, None if invalid
            
        Supports:
        - datetime objects
        - String dates in common formats 
        - Handles invalid dates
        """
        if pd.isna(date_value):
            return None
        
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        
        if isinstance(date_value, str):
            try:
                # Try multiple common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']:
                    try:
                        return datetime.strptime(date_value, fmt).strftime('%Y-%m-%d')
                    except ValueError:
                        continue
            except Exception:
                return None
        
        return None
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate data meets standard format requirements
        
        Args:
            df: DataFrame to validate
            
        Returns:
            bool: Whether validation passed
            
        Raises:
            ValueError: If validation fails
            
        Checks:
        - Required columns exist
        - Commission amounts are numeric
        """
        print("\nExecuting data validation...")
        
        # Check required columns
        required_columns = {
            'carrier_name', 
            'commission_period', 
            'agent_name', 
            'commission_amount'
        }
        
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            print(f"Current columns: {df.columns.tolist()}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Validate commission amounts
        invalid_amounts = ~pd.to_numeric(df['commission_amount'], errors='coerce').notna()
        if invalid_amounts.any():
            print(f"Warning: Found {invalid_amounts.sum()} invalid commission amount records")
            df.loc[invalid_amounts, 'commission_amount'] = 0.0
        
        print("Data validation complete")
        return True
    
    def get_standard_columns(self) -> List[str]:
        """
        Get list of standardized column names
        
        Returns:
            List[str]: Standard column names
            
        Standard columns:
        - Core identification fields
        - Agent and member information
        - Commission details  
        - Dates and other metadata
        """
        return [
            'carrier_name',          # Insurance carrier name
            'commission_period',      # Commission period
            'agent_name',            # Agent name
            'agency_name',           # Agency name
            'member_id',             # Member ID
            'member_name',           # Member name
            'plan_name',             # Plan name
            'enrollment_date',       # Enrollment date
            'disenrollment_date',    # Disenrollment date
            'commission_amount',     # Commission amount
            'transaction_type',      # Transaction type
            'policy_number',         # Policy number
            'effective_date',        # Effective date
            'processed_date'         # Processing date
        ]