#!/usr/bin/env python3
"""
Script per addestramento modello
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np

from src.feature_engineering.temporal_features import TemporalFeatureEngineer
from src.models.xgboost_model import XGBoostModel
from src.models.model_evaluator import ModelEvaluator
from config.settings import ML_CONFIG, MODELS_DIR


def main():
    parser = argparse.ArgumentParser(description='Train ML model')
    parser.add_argument('--model', default='xgboost', choices=['xgboost', 'random_forest'])
    parser.add_argument('--data-path', default='data/processed/matches_merged.csv')
    parser.add_argument('--test-split', default=ML_CONFIG['training']['test_split_date'])
    parser.add_argument('--output-dir', help='Directory to save model')
    
    args = parser.parse_args()
    
    print("Loading data...")
    df = pd.read_csv(args.data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Split temporalmente
    train_df = df[df['date'] < args.test_split]
    test_df = df[df['date'] >= args.test_split]
    
    print(f"Train: {len(train_df)} matches, Test: {len(test_df)} matches")
    
    # Feature engineering
    print("Creating features...")
    feature_engineer = TemporalFeatureEngineer(window_sizes=[5, 10, 20])
    
    # Per ogni partita, crea features usando solo dati precedenti
    train_features = []
    train_targets = []
    
    for idx, match in train_df.iterrows():
        # Get features up to match date
        team_features = feature_engineer.create_team_features(
            train_df[train_df['date'] < match['date']],
            match['date']
        )
        
        # Merge home and away features
        home_features = team_features[team_features['team'] == match['home_team']]
        away_features = team_features[team_features['team'] == match['away_team']]
        
        if not home_features.empty and not away_features.empty:
            # Combine features
            match_features = {
                **home_features.iloc[0].to_dict(),
                **{f'away_{k}': v for k, v in away_features.iloc[0].to_dict()
                   if k != 'team'}
            }
            
            train_features.append(match_features)
            train_targets.append(match['result'])  # H/D/A
    
    # Convert to DataFrame
    X_train = pd.DataFrame(train_features)
    y_train = pd.Series(train_targets)
    
    # Encode target
    y_train_encoded = y_train.map({'H': 0, 'D': 1, 'A': 2})
    
    # Train model
    print("Training model...")
    if args.model == 'xgboost':
        model = XGBoostModel(ML_CONFIG['models']['xgboost'])
    else:
        from src.models.random_forest import RandomForestModel
        model = RandomForestModel(ML_CONFIG['models']['random_forest'])
    
    # Split training data for validation
    val_split = int(len(X_train) * 0.8)
    X_train_split = X_train.iloc[:val_split]
    y_train_split = y_train_encoded.iloc[:val_split]
    X_val_split = X_train.iloc[val_split:]
    y_val_split = y_train_encoded.iloc[val_split:]
    
    model.train(X_train_split, y_train_split, X_val_split, y_val_split)
    
    # Evaluate
    print("Evaluating model...")
    evaluator = ModelEvaluator(model)
    metrics = evaluator.evaluate(X_val_split, y_val_split)
    
    print(f"Validation Accuracy: {metrics['accuracy']:.3f}")
    print(f"Validation Log Loss: {metrics['log_loss']:.3f}")
    
    # Save model
    output_dir = Path(args.output_dir) if args.output_dir else \
                 MODELS_DIR / args.model / datetime.now().strftime('%Y%m%d_%H%M%S')
    
    model.save(output_dir)
    print(f"Model saved to {output_dir}")


if __name__ == "__main__":
    main()
