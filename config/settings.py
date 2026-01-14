# config/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

# ============ 1. CARICAMENTO VARIABILI AMBIENTE ============
# Trova la cartella principale del progetto (football_ml_system)
BASE_DIR = Path(__file__).resolve().parent.parent
# Carica le variabili dal file .env nella cartella principale
load_dotenv(BASE_DIR / ".env")

# ============ 2. PATH DEL PROGETTO ============
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
LOGS_DIR = BASE_DIR / "logs"

# ============ 3. API KEYS (Caricate dal .env) ============
API_KEYS = {
    "football_data": os.getenv("FOOTBALL_DATA_API_KEY", "").strip(),
    "the_odds_api": os.getenv("THE_ODDS_API_KEY", "").strip(),
}

# DEBUG: Verifica che la chiave sia stata caricata
if not API_KEYS["football_data"]:
    print("❌ ATTENZIONE CRITICA: FOOTBALL_DATA_API_KEY non trovata o vuota nel file .env")
    print(f"   Percorso .env cercato: {BASE_DIR / '.env'}")
else:
    print(f"✅ API key caricata (primi 5 caratteri): {API_KEYS['football_data'][:5]}...")

# ============ 4. LISTA COMPETIZIONI ============
# Questa è la variabile COMPETITIONS che lo script cerca di importare
COMPETITIONS = [
    {
        "name": "Serie A",
        "football_data_id": 2019,
        "understat_code": "Serie_A",
        "country": "Italy",
    },
    {
        "name": "Premier League",
        "football_data_id": 2021,
        "understat_code": "EPL",
        "country": "England",
    },
    {
        "name": "La Liga",
        "football_data_id": 2014,
        "understat_code": "La_liga",
        "country": "Spain",
    },
    # Aggiungi altre competizioni qui quando vuoi
]

# ============ 5. CONFIGURAZIONE GENERALE ============
# Esempio: puoi aggiungere altre impostazioni qui in futuro
CONFIG = {
    "database_url": os.getenv("DATABASE_URL", "sqlite:///data/database/soccer_data.db"),
    "log_level": "INFO",
}
