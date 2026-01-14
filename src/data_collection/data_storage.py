# src/data_collection/data_storage.py
import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, List
import os

logger = logging.getLogger(__name__)

class DataStorage:
    """Gestisce il salvataggio dei dati in SQLite."""
    
    def __init__(self, db_path: str = None):
        # Usa il percorso dal .env o un default
        if db_path is None:
            # Importante: leggi la variabile che hai impostato nel .env
            db_path = os.getenv('DATABASE_URL', 'sqlite:///data/database/soccer_data.db')
            # Se la stringa inizia con 'sqlite:///', estrai il percorso file
            if db_path.startswith('sqlite:///'):
                db_path = db_path.replace('sqlite:///', '')
        
        self.db_path = db_path
        # Assicurati che la cartella del database esista
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Crea le tabelle se non esistono."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabella principale partite (versione semplificata)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                match_id TEXT PRIMARY KEY,
                date TIMESTAMP NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                home_score INTEGER,
                away_score INTEGER,
                home_xg REAL,
                away_xg REAL,
                competition TEXT,
                season TEXT,
                source TEXT,
                raw_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Crea un indice per ricerche rapide per data
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date)')
        
        conn.commit()
        conn.close()
        logger.info(f"Database inizializzato: {self.db_path}")
    
    def save_matches(self, matches: List[Dict], source: str = 'unknown'):
        """Salva una lista di partite nel database."""
        if not matches:
            logger.warning("Nessuna partita da salvare.")
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
    def save_matches(self, matches: List[Dict], source: str = 'unknown'):
        """Salva una lista di partite nel database con gestione robusta dei tipi."""
        if not matches:
            logger.warning("Nessuna partita da salvare.")
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        error_count = 0
        
        for match in matches:
            try:
                # Usa l'ID della partita se esiste, altrimenti generane uno
                match_id = match.get('match_id', self._generate_match_id(match))
                
                # --- CONVERSIONE DATI CRITICA ---
                # 1. Gestione data: converti Timestamp in stringa ISO
                date_value = match.get('date')
                if date_value is not None:
                    # Se è un pandas Timestamp, converti in stringa
                    if hasattr(date_value, 'isoformat'):
                        date_str = date_value.isoformat(sep=' ', timespec='seconds')
                    elif isinstance(date_value, str):
                        date_str = date_value
                    else:
                        # Prova conversione generica
                        date_str = str(date_value)
                else:
                    # Data mancante: usa un valore di default o salta
                    logger.warning(f"Match {match_id} senza data. Uso '1900-01-01'.")
                    date_str = '1900-01-01 00:00:00'
                
                # 2. Converti xG a float (se sono stringhe)
                home_xg = match.get('home_xg')
                away_xg = match.get('away_xg')
                
                if home_xg is not None:
                    try:
                        home_xg = float(home_xg)
                    except (ValueError, TypeError):
                        home_xg = 0.0
                
                if away_xg is not None:
                    try:
                        away_xg = float(away_xg)
                    except (ValueError, TypeError):
                        away_xg = 0.0
                
                # 3. Converti punteggi a int
                home_score = match.get('home_score')
                away_score = match.get('away_score')
                
                if home_score is not None:
                    try:
                        home_score = int(home_score)
                    except (ValueError, TypeError):
                        home_score = None
                
                if away_score is not None:
                    try:
                        away_score = int(away_score)
                    except (ValueError, TypeError):
                        away_score = None
                
                # 4. Esegui l'inserimento con dati convertiti
                cursor.execute('''
                    INSERT OR REPLACE INTO matches 
                    (match_id, date, home_team, away_team, home_score, away_score, 
                     home_xg, away_xg, competition, season, source, raw_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_id,
                    date_str,  # Ora è una stringa
                    match.get('home_team'),
                    match.get('away_team'),
                    home_score,
                    away_score,
                    home_xg,
                    away_xg,
                    match.get('competition'),
                    match.get('season'),
                    source,
                    json.dumps(match, default=str)
                ))
                
                saved_count += 1
                    
            except Exception as e:
                error_count += 1
                # Log dettagliato per debug
                logger.error(f"Errore salvataggio match {match_id}: {e}")
                logger.debug(f"Dati problematici: {match}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"Salvataggio completato: {saved_count} salvate, {error_count} errori")
    
    def _generate_match_id(self, match: Dict) -> str:
        """Genera un ID unico per la partita."""
        date = str(match.get('date', ''))[:10].replace('-', '')
        home = match.get('home_team', '').replace(' ', '_')
        away = match.get('away_team', '').replace(' ', '_')
        return f"{date}_{home}_vs_{away}"
