"""
Classe base astratta per tutti i collector di dati
"""
import abc
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

import requests
import pandas as pd

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class BaseDataCollector(abc.ABC):
    """Base class for all data collectors"""
    
    def __init__(self, cache_dir: Path, rate_limit: float = 1.0):
        self.cache_dir = cache_dir
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.session = requests.Session()
        
        # Create cache directory
        cache_dir.mkdir(parents=True, exist_ok=True)
    
    @abc.abstractmethod
    def get_competitions(self) -> List[Dict]:
        """Get list of available competitions"""
        pass
    
    @abc.abstractmethod
    def get_matches(self, competition_id: str, season: str = None) -> List[Dict]:
        """Get matches for a competition"""
        pass
    
    @abc.abstractmethod
    def get_match_details(self, match_id: str) -> Dict:
        """Get details for a specific match"""
        pass
    
    def _rate_limit(self):
        """Apply rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Dict = None,
                     use_cache: bool = True, cache_ttl: int = 3600) -> Optional[Dict]:
        """
        Make HTTP request with caching and rate limiting
        """
        # Generate cache key
        cache_key = self._generate_cache_key(url, params)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        # Try cache first
        if use_cache and cache_file.exists():
            cached_data = self._load_from_cache(cache_file, cache_ttl)
            if cached_data is not None:
                logger.debug(f"Using cached data for {url}")
                return cached_data
        
        # Apply rate limiting
        self._rate_limit()
        
        try:
            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if use_cache:
                    self._save_to_cache(cache_file, data)
                return data
            elif response.status_code == 429:  # Rate limit
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit hit. Waiting {retry_after} seconds")
                time.sleep(retry_after)
                return self._make_request(url, params, use_cache, cache_ttl)
            else:
                logger.error(f"Error {response.status_code} for {url}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            return None
    
    def _generate_cache_key(self, url: str, params: Dict = None) -> str:
        """Generate cache key from URL and params"""
        import hashlib
        key_string = f"{url}_{str(params)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _load_from_cache(self, cache_file: Path, max_age_seconds: int) -> Optional[Dict]:
        """Load data from cache if not too old"""
        if not cache_file.exists():
            return None
        
        file_age = time.time() - cache_file.stat().st_mtime
        if file_age > max_age_seconds:
            return None
        
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading cache {cache_file}: {e}")
            return None
    
    def _save_to_cache(self, cache_file: Path, data: Dict):
        """Save data to cache"""
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving cache {cache_file}: {e}")
    
    def _get_headers(self) -> Dict:
        """Get HTTP headers for requests"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        }
