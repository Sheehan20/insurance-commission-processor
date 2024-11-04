"""
Parser module for processing commission data files from different carriers
"""

from .base_parser import BaseCommissionParser
from .centene_parser import CenteneParser
from .emblem_parser import EmblemParser
from .healthfirst_parser import HealthfirstParser

__all__ = [
    'BaseCommissionParser',
    'CenteneParser',
    'EmblemParser',
    'HealthfirstParser'
]

# Parser registry
PARSER_REGISTRY = {
    'Centene': CenteneParser,
    'Emblem': EmblemParser,
    'Healthfirst': HealthfirstParser
}

def get_parser_for_carrier(carrier_name: str) -> type:
    """
    Get parser class for specified carrier
    
    Args:
        carrier_name: Insurance carrier name
        
    Returns:
        Parser class
    """
    return PARSER_REGISTRY.get(carrier_name)