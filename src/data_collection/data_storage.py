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
        
    def save_match_data(self, matches: List[Dict]):
        """Salva i dati delle partite nel database"""
        if not matches:
            return

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            for match in matches:
                try:
                    # --- MODIFICA: Aggiungi questo controllo ---
                    # Salta la partita se mancano i nomi delle squadre
                    if not match.get('home_team') or not match.get('away_team'):
                        print(f"⚠️ Match ignorato: dati incompleti (home o away mancanti)")
                        continue
                    # -------------------------------------------

                    # Verifica data (codice esistente)
                    match_date = match.get('date')
                    if not match_date:
                        print(f"Match {match.get('home_team', '')}_{match.get('away_team', '')} senza data. Uso '1900-01-01'.")
                        match_date = '1900-01-01'
                    
                    # ... resto del codice (cursor.execute, etc.) ...
                    cursor.execute("""
                        INSERT OR REPLACE INTO matches (
                            date, home_team, away_team, 
                            home_score, away_score, 
                            competition, season,
                            home_xg, away_xg,
                            home_odds, draw_odds, away_odds
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        match_date,
                        match.get('home_team'),
                        match.get('away_team'),
                        match.get('home_score'),
                        match.get('away_score'),
                        match.get('competition'),
                        match.get('season'),
                        match.get('home_xg'),
                        match.get('away_xg'),
                        match.get('home_odds'),
                        match.get('draw_odds'),
                        match.get('away_odds')
                    ))
                except Exception as e:
                    print(f"Errore salvataggio match {match.get('home_team', '')}_{match.get('away_team', '')}: {str(e)}")
            
            conn.commit()
            print("✅ Salvataggio completato.") # Opzionale: conferma visiva
            
        except Exception as e:
            print(f"Errore durante il salvataggio nel DB: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _generate_match_id(self, match: Dict) -> str:
        """Genera un ID unico per la partita."""
        date = str(match.get('date', ''))[:10].replace('-', '')
        home = match.get('home_team', '').replace(' ', '_')
        away = match.get('away_team', '').replace(' ', '_')
        return f"{date}_{home}_vs_{away}"
