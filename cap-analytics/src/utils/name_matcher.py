"""
Name Matching and Standardization Utilities
Handles cleaning, matching and deduplication of agent and agency names
"""

import re
from typing import List, Tuple, Set
from difflib import SequenceMatcher
import pandas as pd

def normalize_name(name: str) -> str:
    """
    Standardize name format
    
    Args:
        name: Input name
        
    Returns:
        str: Standardized name
        
    Standardization steps:
    1. Convert to lowercase
    2. Remove extra whitespace
    3. Remove special characters
    4. Remove common business suffixes
    """
    if pd.isna(name):
        return ""
    
    name = str(name).lower().strip()
    name = " ".join(name.split())
    name = re.sub(r'[^\w\s-]', '', name)
    
    # Remove common business suffixes
    replacements = {
        ' inc ': ' ',
        ' incorporated ': ' ',
        ' corp ': ' ',
        ' corporation ': ' ',
        ' llc ': ' ',
        ' ltd ': ' ',
        ' limited ': ' '
    }
    
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    return name.strip()

def are_similar_names(name1: str, name2: str, threshold: float = 0.85) -> bool:
    """
    Check if two names are similar
    
    Args:
        name1: First name
        name2: Second name  
        threshold: Similarity threshold (0-1)
        
    Returns:
        bool: Whether names are similar
        
    Matching logic:
    1. Normalize both names
    2. Check exact match
    3. Check substring containment
    4. Compare common words
    5. Calculate string similarity ratio
    """
    name1 = normalize_name(name1)
    name2 = normalize_name(name2)
    
    if not name1 or not name2:
        return False
    
    if name1 == name2:
        return True
    
    if name1 in name2 or name2 in name1:
        return True
    
    words1 = set(name1.split())
    words2 = set(name2.split())
    common_words = words1.intersection(words2)
    
    if len(common_words) >= min(len(words1), len(words2)):
        return True
    
    similarity = SequenceMatcher(None, name1, name2).ratio()
    return similarity >= threshold

def match_names(names: List[str], threshold: float = 0.85) -> List[Tuple[str, str]]:
    """
    Find all similar name pairs in a list
    
    Args:
        names: List of names to compare
        threshold: Similarity threshold
        
    Returns:
        List[Tuple[str, str]]: List of similar name pairs
    """
    matches = []
    normalized_names = [(name, normalize_name(name)) for name in names]
    
    for i, (name1, norm1) in enumerate(normalized_names):
        for name2, norm2 in normalized_names[i+1:]:
            if are_similar_names(norm1, norm2, threshold):
                matches.append((name1, name2))
    
    return matches

def find_canonical_name(names: List[str]) -> str:
    """
    Select a canonical name from a group of similar names
    
    Args:
        names: List of similar names
        
    Returns:
        str: Selected canonical name
        
    Selection criteria:
    1. Filter out invalid names
    2. Sort by number of words, then total length
    3. Select shortest name as canonical
    """
    if not names:
        return ""
    
    valid_names = [name for name in names if name and not pd.isna(name)]
    if not valid_names:
        return ""
    
    sorted_names = sorted(valid_names, key=lambda x: (len(x.split()), len(x)))
    return sorted_names[0]

def group_similar_names(names: List[str], threshold: float = 0.85) -> dict:
    """
    Group similar names together
    
    Args:
        names: List of names to group
        threshold: Similarity threshold
        
    Returns:
        dict: Mapping from canonical names to lists of similar names
        
    Process:
    1. Find groups of similar names
    2. Select canonical name for each group
    3. Track processed names to avoid duplicates
    """
    groups = {}
    processed = set()
    
    for name in names:
        if name in processed:
            continue
            
        similar_names = {name}
        for other_name in names:
            if other_name != name and other_name not in processed:
                if are_similar_names(name, other_name, threshold):
                    similar_names.add(other_name)
        
        if similar_names:
            canonical = find_canonical_name(list(similar_names))
            groups[canonical] = list(similar_names)
            processed.update(similar_names)
    
    return groups