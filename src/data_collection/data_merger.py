# src/data_collection/data_merger.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class DataMerger:
    """Fonde dati partite da diverse fonti con logica intelligente."""
    
    def __init__(self):
        self.team_normalizer = TeamNameNormalizer()
        self._setup_logging()
    
    def _setup_logging(self):
        """Configura logging per il merger."""
        self.logger = logging.getLogger(__name__)
    
    def merge_matches(self, *sources: List[List[Dict]],
                     primary_source: str = 'football_data') -> List[Dict]:
        """
        Fonde dati partite da multiple fonti con risoluzione intelligente.
        
        Args:
            *sources: Liste di partite da diverse fonti
            primary_source: Fonte primaria per risolvere conflitti
            
        Returns:
            Lista di partite fuse
        """
        if not sources:
            self.logger.warning("Nessun dato da fondere")
            return []
        
        # Converti ogni fonte in DataFrame
        dfs = []
        source_names = ['football_data', 'understat', 'sofascore']  # Adatta se necessario
        
        for idx, source_matches in enumerate(sources):
            if not source_matches:
                continue
                
            df = pd.DataFrame(source_matches)
            if len(df) == 0:
                continue
                
            # Normalizza nomi squadre
            df = self._normalize_dataframe(df)
            df['_source'] = source_names[idx] if idx < len(source_names) else f'source_{idx}'
            dfs.append(df)
        
        if not dfs:
            self.logger.error("Tutte le fonti sono vuote")
            return []
        
        # Combina tutti i DataFrame
        combined = pd.concat(dfs, ignore_index=True, sort=False)
        
        # Crea chiave unica per ogni partita
        combined['_match_key'] = combined.apply(
            lambda row: self._create_match_key(
                row.get('date'),
                row.get('home_team'),
                row.get('away_team')
            ), axis=1
        )
        
        # Raggruppa per partita e fondi
        merged_matches = []
        
        for match_key, group in combined.groupby('_match_key'):
            if len(group) > 1:
                # Fusione intelligente multi-fonte
                merged_match = self._merge_match_group(group, primary_source)
            else:
                # Singola fonte
                merged_match = group.iloc[0].to_dict()
            
            # Pulisci campi temporanei
            merged_match.pop('_source', None)
            merged_match.pop('_match_key', None)
            
            merged_matches.append(merged_match)
        
        self.logger.info(f"Fusione completata: {len(merged_matches)} partite da {len(dfs)} fonti")
        return merged_matches
    
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalizza nomi squadre e formatta date."""
        df = df.copy()
        
        # Normalizza nomi squadre
        if 'home_team' in df.columns:
            df['home_team'] = df['home_team'].apply(self.team_normalizer.normalize)
        if 'away_team' in df.columns:
            df['away_team'] = df['away_team'].apply(self.team_normalizer.normalize)
        
        # Normalizza date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        return df
    
    def _create_match_key(self, date, home_team: str, away_team: str) -> str:
        """Crea chiave unica per una partita."""
        if pd.isna(date):
            date_str = 'unknown'
        else:
            # Usa solo la data, non l'ora
            if isinstance(date, datetime):
                date_str = date.strftime('%Y-%m-%d')
            else:
                date_str = str(date)[:10]
        
        home_norm = self.team_normalizer.normalize(home_team or '')
        away_norm = self.team_normalizer.normalize(away_team or '')
        
        # Ordina le squadre per chiave consistente
        teams = sorted([home_norm, away_norm])
        return f"{date_str}_{teams[0]}_vs_{teams[1]}"
    
    def _merge_match_group(self, group: pd.DataFrame, primary_source: str) -> Dict:
        """
        Fusione intelligente di una partita da multiple fonti.
        
        Strategia:
        1. Prendi dati dalla fonte primaria come base
        2. Integra dati mancanti da altre fonti
        3. Per conflitti, usa logica specifica per campo
        """
        # Ordina per priorità fonte
        source_priority = {
            'football_data': 1,
            'understat': 2,
            'sofascore': 3,
            primary_source: 0  # Massima priorità
        }
        
        group['_priority'] = group['_source'].apply(
            lambda x: source_priority.get(x, 999)
        )
        group = group.sort_values('_priority')
        
        merged = {}
        processed_columns = set()
        
        # Per ogni colonna, prendi il valore dalla fonte con priorità più alta
        for _, row in group.iterrows():
            for column in group.columns:
                if column.startswith('_'):
                    continue
                    
                value = row[column]
                if pd.notna(value) and value not in [None, '', []]:
                    if column not in merged or column in ['match_id', 'date', 'home_team', 'away_team']:
                        # Campi base: prendi dalla prima fonte
                        merged[column] = value
                    elif column in ['home_xg', 'away_xg', 'home_shots', 'away_shots']:
                        # Statistiche: preferisci understat per xG
                        if 'understat' in row['_source'] and 'xg' in column.lower():
                            merged[column] = value
                        elif column not in merged:
                            merged[column] = value
                    elif column not in merged:
                        # Altri campi: prendi il primo valore valido
                        merged[column] = value
        
        # Gestione speciale per campi derivati
        self._calculate_derived_fields(merged)
        
        return merged
    
    def _calculate_derived_fields(self, match: Dict):
        """Calcola campi derivati (goal totali, risultato, ecc.)."""
        try:
            # Risultato (H/D/A)
            if 'home_score' in match and 'away_score' in match:
                h_score = match['home_score']
                a_score = match['away_score']
                
                if h_score is not None and a_score is not None:
                    match['total_goals'] = h_score + a_score
                    match['goal_difference'] = h_score - a_score
                    
                    if h_score > a_score:
                        match['result'] = 'H'
                    elif h_score < a_score:
                        match['result'] = 'A'
                    else:
                        match['result'] = 'D'
            
            # xG totale
            if 'home_xg' in match and 'away_xg' in match:
                h_xg = match['home_xg']
                a_xg = match['away_xg']
                
                if h_xg is not None and a_xg is not None:
                    match['total_xg'] = float(h_xg) + float(a_xg)
                    match['xg_difference'] = float(h_xg) - float(a_xg)
                    
        except (TypeError, ValueError) as e:
            self.logger.debug(f"Errore calcolo campi derivati: {e}")


class TeamNameNormalizer:
    """Normalizza nomi squadre per matching tra diverse fonti."""
    
    def __init__(self):
        self.mappings = self._load_mappings()
        self._build_alias_dict()
    
    def _load_mappings(self) -> Dict[str, List[str]]:
        """Carica mapping manuale nomi squadre."""
        return {
            'Manchester United': ['Man United', 'Manchester Utd', 'Man Utd'],
            'Manchester City': ['Man City', 'Manchester C'],
            'Tottenham Hotspur': ['Tottenham', 'Spurs'],
            'West Ham United': ['West Ham', 'West Ham Utd'],
            'Newcastle United': ['Newcastle', 'Newcastle Utd'],
            'Brighton & Hove Albion': ['Brighton', 'Brighton Hove'],
            'Wolverhampton Wanderers': ['Wolves', 'Wolverhampton'],
            'AC Milan': ['Milan'],
            'Inter Milan': ['Inter'],
            'AS Roma': ['Roma'],
            'SS Lazio': ['Lazio'],
            'Atlético Madrid': ['Atletico Madrid', 'Atletico'],
            'Athletic Bilbao': ['Athletic Club'],
            'Paris Saint-Germain': ['PSG', 'Paris SG'],
            'Bayern Munich': ['Bayern München', 'Bayern'],
            'Borussia Dortmund': ['Dortmund'],
            'Juventus': ['Juve'],
            'Napoli': ['SSC Napoli'],
        }
    
    def _build_alias_dict(self):
        """Crea dizionario inverso alias->nome standard."""
        self.alias_to_standard = {}
        for standard, aliases in self.mappings.items():
            self.alias_to_standard[standard.lower()] = standard
            for alias in aliases:
                self.alias_to_standard[alias.lower()] = standard
    
    def normalize(self, team_name: str) -> str:
        """
        Normalizza nome squadra a versione standard.
        
        Args:
            team_name: Nome originale
            
        Returns:
            Nome normalizzato
        """
        if not team_name or pd.isna(team_name):
            return ""
        
        # Controllo diretto nei mappings
        team_lower = team_name.strip().lower()
        
        if team_lower in self.alias_to_standard:
            return self.alias_to_standard[team_lower]
        
        # Rimuovi prefissi/suffissi comuni
        normalized = team_lower
        
        prefixes = ['fc ', 'afc ', 'ssc ', 'as ', 'fk ', 'sc ', 'rc ', 'ud ']
        suffixes = [' fc', ' afc', ' ssc', ' as', ' cf']
        
        for prefix in prefixes:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
        
        for suffix in suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
        
        # Cerca match parziale
        for standard, aliases in self.mappings.items():
            all_names = [standard.lower()] + [a.lower() for a in aliases]
            for name in all_names:
                if self._similarity(normalized, name) > 0.85:
                    return standard
        
        # Capitalizza prima lettera di ogni parola
        return normalized.title()
    
    def _similarity(self, a: str, b: str) -> float:
        """Calcola similarità tra due stringhe."""
        return SequenceMatcher(None, a, b).ratio()
    
    def find_best_match(self, team_name: str, candidates: List[str]) -> Optional[str]:
        """
        Trova il miglior match per un nome squadra tra candidati.
        
        Args:
            team_name: Nome da matchare
            candidates: Lista di nomi candidati
            
        Returns:
            Miglior match o None
        """
        if not team_name or not candidates:
            return None
        
        normalized_target = self.normalize(team_name)
        best_match = None
        best_score = 0
        
        for candidate in candidates:
            normalized_candidate = self.normalize(candidate)
            score = self._similarity(normalized_target, normalized_candidate)
            
            if score > best_score and score > 0.8:
                best_score = score
                best_match = candidate
        
        return best_match
