# src/feature_engineering/temporal_features.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TemporalFeatureEngineer:
    """Crea features basate sulle performance recenti delle squadre."""
    
    def __init__(self, window_sizes: List[int] = [5, 10]):
        self.window_sizes = window_sizes
    
    def create_team_features(self, matches_df: pd.DataFrame,
                            date_cutoff: datetime) -> pd.DataFrame:
        """
        Crea features per ogni squadra usando solo partite PRIMA della data cutoff.
        
        Args:
            matches_df: DataFrame con tutte le partite storiche
            date_cutoff: Data limite (non includere partite dopo questa data)
            
        Returns:
            DataFrame con features per squadra
        """
        # Filtra partite storiche
        historical = matches_df[matches_df['date'] < date_cutoff].copy()
        
        if historical.empty:
            logger.warning(f"Nessuna partita storica prima di {date_cutoff}")
            return pd.DataFrame()
        
        # Raccogli features per ogni squadra
        all_teams = pd.concat([historical['home_team'], historical['away_team']]).unique()
        features_list = []
        
        for team in all_teams:
            team_features = self._calculate_features_for_team(team, historical)
            features_list.append(team_features)
        
        return pd.DataFrame(features_list)
    
    def _calculate_features_for_team(self, team: str,
                                    historical_df: pd.DataFrame) -> Dict:
        """Calcola tutte le features per una singola squadra."""
        # Filtra partite della squadra
        team_matches = historical_df[
            (historical_df['home_team'] == team) |
            (historical_df['away_team'] == team)
        ].sort_values('date', ascending=False)
        
        features = {'team': team, 'total_matches': len(team_matches)}
        
        if len(team_matches) == 0:
            return self._get_default_features(team)
        
        # Per ogni finestra temporale (es: ultime 5, 10 partite)
        for window in self.window_sizes:
            recent = team_matches.head(window)
            if len(recent) > 0:
                prefix = f'last{window}_'
                
                # Calcola statistiche base
                goals_scored, goals_conceded, points = [], [], []
                
                for _, match in recent.iterrows():
                    if match['home_team'] == team:
                        goals_scored.append(match.get('home_score', 0))
                        goals_conceded.append(match.get('away_score', 0))
                        # Punti: 3=vittoria, 1=pareggio, 0=sconfitta
                        if match.get('home_score', 0) > match.get('away_score', 0):
                            points.append(3)
                        elif match.get('home_score', 0) == match.get('away_score', 0):
                            points.append(1)
                        else:
                            points.append(0)
                    else:  # away
                        goals_scored.append(match.get('away_score', 0))
                        goals_conceded.append(match.get('home_score', 0))
                        if match.get('away_score', 0) > match.get('home_score', 0):
                            points.append(3)
                        elif match.get('away_score', 0) == match.get('home_score', 0):
                            points.append(1)
                        else:
                            points.append(0)
                
                # Aggiungi features
                features[f'{prefix}goals_scored_avg'] = np.mean(goals_scored) if goals_scored else 0
                features[f'{prefix}goals_conceded_avg'] = np.mean(goals_conceded) if goals_conceded else 0
                features[f'{prefix}points_avg'] = np.mean(points) if points else 0
                features[f'{prefix}win_rate'] = sum(1 for p in points if p == 3) / len(points) if points else 0
                
                # xG se disponibile
                if 'home_xg' in recent.columns:
                    xg_for, xg_against = [], []
                    for _, match in recent.iterrows():
                        if match['home_team'] == team:
                            xg_for.append(match.get('home_xg', 0))
                            xg_against.append(match.get('away_xg', 0))
                        else:
                            xg_for.append(match.get('away_xg', 0))
                            xg_against.append(match.get('home_xg', 0))
                    
                    features[f'{prefix}xg_for_avg'] = np.mean(xg_for) if xg_for else 0
                    features[f'{prefix}xg_against_avg'] = np.mean(xg_against) if xg_against else 0
        
        return features
    
    def _get_default_features(self, team: str) -> Dict:
        """Features di default per squadre senza storia."""
        return {
            'team': team,
            'total_matches': 0,
            'last5_goals_scored_avg': 1.0,
            'last5_goals_conceded_avg': 1.0,
            'last5_points_avg': 1.0,
            'last5_win_rate': 0.33,
        }
