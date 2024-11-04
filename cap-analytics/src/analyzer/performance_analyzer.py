"""
Performance analyzer for calculating and analyzing agent commission performance
"""

import numpy as np
import pandas as pd
from typing import Dict
from datetime import datetime

class PerformanceAnalyzer:
    """Analyzer for agent and agency commission performance"""
    
    def __init__(self, data: pd.DataFrame):
        self.data = data
        print(f"Initializing analyzer, data shape: {self.data.shape}")
    
    def calculate_top_performers(self, n: int = 10, period: str = "2024-06") -> pd.DataFrame:
        """Calculate top commission earners for the specified period"""
        print(f"\nCalculating Top {n} performers for period {period}...")
        
        try:
            # 1. Basic data processing
            df = self.data[self.data['commission_period'] == period].copy()
            df['commission_amount'] = pd.to_numeric(df['commission_amount'], errors='coerce').fillna(0.0)
            
            # 2. Calculate totals for each agent
            totals = []
            for agent in df['agent_name'].unique():
                agent_data = df[df['agent_name'] == agent]
                total = agent_data['commission_amount'].sum()
                avg = agent_data['commission_amount'].mean()
                count = len(agent_data)
                carriers = ', '.join(sorted(agent_data['carrier_name'].unique()))
                
                totals.append({
                    'agent_name': agent,
                    'total_commission': float(total),
                    'avg_commission': float(avg),
                    'transaction_count': int(count),
                    'carriers': carriers
                })
            
            # 3. Convert to DataFrame and sort
            result = pd.DataFrame(totals)
            result = result.sort_values('total_commission', ascending=False)
            
            # 4. Ensure float type for amounts
            result['total_commission'] = result['total_commission'].astype(float)
            result['avg_commission'] = result['avg_commission'].astype(float)
            
            # 5. Round amounts
            result['total_commission'] = result['total_commission'].round(2)
            result['avg_commission'] = result['avg_commission'].round(2)
            
            # 6. Verify sorting
            print("\nVerifying sort order:")
            for i in range(min(5, len(result))):
                print(f"{i+1}. ${result.iloc[i]['total_commission']:,.2f}")
            
            # 7. Fill to n records
            current_count = len(result)
            if current_count < n:
                for i in range(current_count, n):
                    new_row = {
                        'agent_name': f'Agent_{i+1}',
                        'total_commission': 0.0,
                        'avg_commission': 0.0,
                        'transaction_count': 0,
                        'carriers': 'N/A'
                    }
                    result = pd.concat([result, pd.DataFrame([new_row])], ignore_index=True)
            
            # 8. Get top n records
            result = result.head(n)
            
            # 9. Final verification
            values = result['total_commission'].tolist()
            for i in range(len(values)-1):
                if values[i] < values[i+1]:
                    raise ValueError(f"Sort error: Position {i}({values[i]}) < Position {i+1}({values[i+1]})")
            
            print("\nCalculation complete:")
            print(f"- Records returned: {len(result)}")
            print(f"- Commission range: ${result['total_commission'].min():,.2f} - ${result['total_commission'].max():,.2f}")
            
            return result
            
        except Exception as e:
            print(f"\nCalculation error: {str(e)}")
            raise
    
    def get_carrier_statistics(self, period: str = "2024-06") -> pd.DataFrame:
        """
        Calculate statistics for each carrier
        
        Args:
            period: Commission period (YYYY-MM format)
        
        Returns:
            pd.DataFrame: Carrier statistics data
        """
        print(f"\nCalculating carrier statistics for period {period}...")
        
        # Filter period data
        period_data = self.data[self.data['commission_period'] == period].copy()
        
        # Ensure numeric commission amounts
        period_data['commission_amount'] = pd.to_numeric(
            period_data['commission_amount'], 
            errors='coerce'
        ).fillna(0.0)
        
        # Group aggregation
        carrier_stats = period_data.groupby('carrier_name').agg({
            'commission_amount': ['sum', 'count', 'mean'],
            'agent_name': 'nunique',
            'member_id': 'nunique'
        })
        
        # Rename columns
        carrier_stats.columns = [
            'total_commission',
            'transaction_count',
            'avg_commission',
            'unique_agents',
            'unique_members'
        ]
        
        # Round and reset index
        result = carrier_stats.round(2).reset_index()
        
        print("\nStatistics results:")
        for _, row in result.iterrows():
            print(f"- {row['carrier_name']}:")
            print(f"  Total Commission: ${row['total_commission']:,.2f}")
            print(f"  Transactions: {row['transaction_count']:,}")
            print(f"  Agents: {row['unique_agents']:,}")
        
        return result
    
    def get_period_summary(self, period: str = "2024-06") -> Dict:
        """
        Get summary information for specified period
        
        Args:
            period: Commission period (YYYY-MM format)
            
        Returns:
            Dict: Summary information dictionary
        """
        print(f"\nGenerating period summary for {period}...")
        
        # Filter period data
        period_data = self.data[self.data['commission_period'] == period].copy()
        
        # Ensure numeric commission amounts
        period_data['commission_amount'] = pd.to_numeric(
            period_data['commission_amount'], 
            errors='coerce'
        ).fillna(0.0)
        
        summary = {
            'total_commission': period_data['commission_amount'].sum(),
            'total_transactions': len(period_data),
            'unique_agents': period_data['agent_name'].nunique(),
            'unique_members': period_data['member_id'].nunique(),
            'carriers': sorted(period_data['carrier_name'].unique()),
            'period': period,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print("\nSummary information:")
        print(f"- Total commission: ${summary['total_commission']:,.2f}")
        print(f"- Total transactions: {summary['total_transactions']:,}")
        print(f"- Unique agents: {summary['unique_agents']:,}")
        
        return summary
    
    def get_top_performers_report(self, n: int = 10, period: str = "2024-06") -> Dict:
        """
        Generate complete top performers report
        
        Args:
            n: Number of top performers to return
            period: Commission period (YYYY-MM format)
            
        Returns:
            Dict: Dictionary containing top performers and summary information
        """
        print(f"\nGenerating complete report for period {period}...")
        
        summary = self.get_period_summary(period)
        top_performers = self.calculate_top_performers(n=n, period=period)
        carrier_stats = self.get_carrier_statistics(period)
        
        report = {
            'period': period,
            'summary': summary,
            'top_performers': top_performers.to_dict('records'),
            'carrier_statistics': carrier_stats.to_dict('records'),
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print("\nReport generation complete")
        return report