"""
Commission Reconciliation Project
"""

from .parser.base_parser import BaseCommissionParser
from .parser.centene_parser import CenteneParser
from .parser.emblem_parser import EmblemParser
from .parser.healthfirst_parser import HealthfirstParser
from .normalizer import DataNormalizer
from .analyzer import PerformanceAnalyzer

__version__ = '1.0.0'

__all__ = [
    'BaseCommissionParser',
    'CenteneParser',
    'EmblemParser',
    'HealthfirstParser',
    'DataNormalizer',
    'PerformanceAnalyzer'
]

# Version info
VERSION_INFO = {
    'major': 1,
    'minor': 0,
    'patch': 0,
    'status': 'stable',
}

# Configuration
DEFAULT_CONFIG = {
    'name_matching_threshold': 0.85,
    'commission_period_format': '%Y-%m',
    'date_format': '%Y-%m-%d',
    'min_commission_amount': 0.0,
    'max_error_threshold': 100
}