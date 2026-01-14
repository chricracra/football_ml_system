# src/data_collection/football_data.py
import logging
from typing import Dict, List, Optional
from pathlib import Path

from .base_collector import BaseDataCollector
from config.settings import API_KEYS  # Assicurati che questa importazione funzioni

logger = logging.getLogger(__name__)

class FootballDataCollector(BaseDataCollector):
    """Collector per Football-Data.org API"""
    
    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self, cache_dir: Path):
        super().__init__(cache_dir, rate_limit=1.0)
        self.api_key = API_KEYS.get("football_data")
        if not self.api_key:
            logger.warning("Football-Data API key non configurata")
    
    # --- METODI ASTRATTI OBBLIGATORI (implementazione base) ---
    
    def get_competitions(self) -> List[Dict]:
        """Ottiene l'elenco delle competizioni."""
        # Implementazione di base: puoi lasciarla vuota per ora o popolarla
        # con le competizioni che ti interessano (es. Serie A ID=2019)
        return [
            {"id": 2019, "name": "Serie A", "code": "SA"},
            {"id": 2021, "name": "Premier League", "code": "PL"},
            # Aggiungi altre competizioni se vuoi
        ]
    
    def get_matches(self, competition_id: str, season: str = None) -> List[Dict]:
        """Ottiene le partite per una competizione e stagione."""
        # Questa è la logica principale di raccolta dati
        url = f"{self.BASE_URL}/competitions/{competition_id}/matches"
        
        params = {}
        if season:
            season_year = season.split('-')[0] if '-' in season else season
            params['season'] = season_year
        
        data = self._make_request(url, params, use_cache=True, cache_ttl=3600)
        
        if data and 'matches' in data:
            return self._parse_matches(data['matches'], competition_id)
        
        logger.error(f"Nessun dato ricevuto per competition_id {competition_id}")
        return []
    
    def get_match_details(self, match_id: str) -> Dict:
        """Ottiene i dettagli per una partita specifica. (IMPLEMENTAZIONE MINIMA)"""
        # Per far funzionare lo script ora, restituiamo un dizionario vuoto.
        # In futuro, potrai implementare una chiamata API vera qui.
        # url = f"{self.BASE_URL}/matches/{match_id}"
        # data = self._make_request(url)
        # return data if data else {}
        logger.debug(f"Richiesti dettagli per match_id {match_id} (non implementato)")
        return {}
    
    # --- ALTRI METODI DI SUPPORTO ---
    
    def _get_headers(self) -> Dict:
        headers = super()._get_headers()
        if self.api_key:
            headers['X-Auth-Token'] = self.api_key
        return headers
    
    def _parse_matches(self, matches_data: List[Dict], competition_id: str) -> List[Dict]:
        """Parsing dei dati delle partite (adatta questa logica ai tuoi bisogni)."""
        parsed_matches = []
        for match in matches_data:
            try:
                parsed = {
                    'match_id': match['id'],
                    'date': match['utcDate'],
                    'home_team': match['homeTeam']['name'],
                    'away_team': match['awayTeam']['name'],
                    'home_score': match['score']['fullTime']['home'],
                    'away_score': match['score']['fullTime']['away'],
                    'competition_id': competition_id,
                    'status': match['status'],
                }
                parsed_matches.append(parsed)
            except KeyError as e:
                logger.warning(f"Campo mancante nella partita {match.get('id')}: {e}")
                continue
        return parsed_matches
