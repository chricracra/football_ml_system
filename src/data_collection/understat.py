# src/data_collection/understat.py
import logging
from typing import Dict, List, Optional
from pathlib import Path

from .base_collector import BaseDataCollector
from understatapi import UnderstatClient

logger = logging.getLogger(__name__)

class UnderstatCollector(BaseDataCollector):
    """Collector per dati xG da Understat.com"""
    
    def __init__(self, cache_dir: Path):
        super().__init__(cache_dir, rate_limit=2.0)  # Rate limit più conservativo per scraping
    
    # --- METODI ASTRATTI OBBLIGATORI ---
    
    def get_competitions(self) -> List[Dict]:
        """Restituisce le competizioni supportate da Understat."""
        return [
            {"id": "Serie_A", "name": "Serie A", "understat_code": "Serie_A"},
            {"id": "EPL", "name": "Premier League", "understat_code": "EPL"},
            {"id": "La_liga", "name": "La Liga", "understat_code": "La_liga"},
            {"id": "Bundesliga", "name": "Bundesliga", "understat_code": "Bundesliga"},
            {"id": "Ligue_1", "name": "Ligue 1", "understat_code": "Ligue_1"},
        ]
    
    def get_matches(self, competition_code: str, season: str = None) -> List[Dict]:
        """Ottiene le partite con xG per una competizione e stagione."""
        if not season:
            logger.error("La stagione è obbligatoria per Understat")
            return []
        
        # Understat vuole la stagione nel formato "2023"
        if '-' in season:
            season = season.split('-')[0]
        
        logger.info(f"Understat: raccolta dati per {competition_code}, stagione {season}")
        
        try:
            # Questa è la sintassi corretta per understatapi
            with UnderstatClient() as understat:
                match_data = understat.league(league=competition_code).get_match_data(season=season)
                # understatapi restituisce già una lista di dizionari
                return match_data
        except Exception as e:
            logger.error(f"Errore nel fetch dati Understat: {e}")
            return []
    
    def get_match_details(self, match_id: str) -> Dict:
        """Ottiene dettagli per una partita specifica. (Implementazione base)"""
        logger.debug(f"Richiesti dettagli Understat per match_id {match_id}")
        return {}  # Per ora restituisce vuoto
