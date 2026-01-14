#!/usr/bin/env python3
"""
Script per raccolta dati
"""
import argparse
import sys
from pathlib import Path

# Aggiungi il percorso src
sys.path.append(str(Path(__file__).parent.parent))

# --- IMPORTAZIONI OBBLIGATORIE ---
from src.data_collection.football_data import FootballDataCollector
from src.data_collection.understat import UnderstatCollector
from src.data_collection.data_merger import DataMerger  # <-- QUESTA È ESSENZIALE
from src.data_collection.data_storage import DataStorage
from config.settings import COMPETITIONS, DATA_DIR

def main():
    parser = argparse.ArgumentParser(description='Collect soccer data')
    parser.add_argument('--competition', help='Specific competition to collect')
    parser.add_argument('--season', help='Specific season (e.g., 2023-2024)')
    
    args = parser.parse_args()
    
    print("=== Inizio raccolta dati ===")
    
    # 1. Inizializza i collector
    print("\n1. Inizializzazione collector...")
    fd_collector = FootballDataCollector(DATA_DIR / "cache" / "football_data")
    us_collector = UnderstatCollector(DATA_DIR / "cache" / "understat")
    
    # 2. Raccogli dati separatamente
    print(f"\n2. Raccolta dati per {args.competition} - {args.season}")
    
    # Football-Data.org
    print("   - Football-Data.org...")
    fd_matches = fd_collector.get_matches(2019, args.season)  # 2019 = ID Serie A
    
    # Understat
    print("   - Understat (xG)...")
    us_matches = us_collector.get_matches("Serie_A", args.season)
    
    # 3. FUSIONE DATI (questa è la parte che ti manca)
    print("\n3. Fusione dati...")
    merger = DataMerger()  # <-- Creazione dell'istanza
    merged_matches = merger.merge_matches(fd_matches, us_matches, primary_source='football_data')
    
    print(f"   Fusionate {len(merged_matches)} partite")
    
    # 4. Salva nel database
    print("\n4. Salvataggio database...")
    storage = DataStorage()
    storage.save_matches(merged_matches, source='merged')
    
    print("\n=== Raccolta completata ===")

if __name__ == "__main__":
    main()
