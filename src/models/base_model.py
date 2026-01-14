"""
Classe base per tutti i modelli ML
"""
import abc
import pickle
import json
from typing import Dict, Any, Tuple
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, log_loss

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class BaseModel(abc.ABC):
    """Base class for all ML models"""
    
    def __init__(self, model_name: str, model_params: Dict = None):
        self.model_name = model_name
        self.model_params = model_params or {}
        self.model = None
        self.feature_importance = None
        self.training_history = []
        
        # Metadata
        self.created_at = datetime.now()
        self.version = "1.0"
        self.feature_columns = []
    
    @abc.abstractmethod
    def _build_model(self):
        """Build the specific model architecture"""
        pass
    
    def train(self, X_train: pd.DataFrame, y_train: pd.Series,
              X_val: pd.DataFrame = None, y_val: pd.Series = None):
        """Train the model"""
        logger.info(f"Training {self.model_name}...")
        
        # Store feature columns
        self.feature_columns = list(X_train.columns)
        
        # Build model
        self.model = self._build_model()
        
        # Fit model
        if X_val is not None and y_val is not None:
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                **self.model_params
            )
        else:
            self.model.fit(X_train, y_train, **self.model_params)
        
        # Calculate feature importance if available
        self._calculate_feature_importance(X_train)
        
        logger.info(f"Training completed for {self.model_name}")
    
    def predict(self, X: pd.DataFrame, return_proba: bool = True) -> np.ndarray:
        """Make predictions"""
        if self.model is None:
            raise ValueError("Model not trained yet")
        
        if return_proba:
            return self.model.predict_proba(X)
        else:
            return self.model.predict(X)
    
    def evaluate(self, X_test: pd.DataFrame, y_test: pd.Series) -> Dict:
        """Evaluate model performance"""
        y_pred_proba = self.predict(X_test, return_proba=True)
        y_pred = np.argmax(y_pred_proba, axis=1)
        
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'log_loss': log_loss(y_test, y_pred_proba),
            'prediction_counts': np.bincount(y_pred),
            'actual_counts': np.bincount(y_test),
        }
        
        return metrics
    
    def _calculate_feature_importance(self, X: pd.DataFrame):
        """Calculate feature importance if model supports it"""
        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = pd.DataFrame({
                'feature': X.columns,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
    
    def save(self, model_dir: Path):
        """Save model to disk"""
        model_dir.mkdir(parents=True, exist_ok=True)
        
        # Save model
        model_path = model_dir / "model.pkl"
        with open(model_path, 'wb') as f:
            pickle.dump(self.model, f)
        
        # Save metadata
        metadata = {
            'model_name': self.model_name,
            'model_params': self.model_params,
            'feature_columns': self.feature_columns,
            'created_at': self.created_at.isoformat(),
            'version': self.version,
            'feature_importance': self.feature_importance.to_dict()
                if self.feature_importance is not None else None,
        }
        
        metadata_path = model_dir / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Model saved to {model_dir}")
    
    @classmethod
    def load(cls, model_dir: Path):
        """Load model from disk"""
        # Load metadata
        metadata_path = model_dir / "metadata.json"
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        # Create model instance
        model_instance = cls(
            model_name=metadata['model_name'],
            model_params=metadata['model_params']
        )
        
        # Load model
        model_path = model_dir / "model.pkl"
        with open(model_path, 'rb') as f:
            model_instance.model = pickle.load(f)
        
        # Restore metadata
        model_instance.feature_columns = metadata['feature_columns']
        model_instance.created_at = datetime.fromisoformat(metadata['created_at'])
        model_instance.version = metadata['version']
        
        if metadata['feature_importance']:
            model_instance.feature_importance = pd.DataFrame(
                metadata['feature_importance']
            )
        
        return model_instance
