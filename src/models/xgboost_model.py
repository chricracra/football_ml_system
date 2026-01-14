"""
XGBoost model for match predictions
"""
import xgboost as xgb
from typing import Dict

from .base_model import BaseModel


class XGBoostModel(BaseModel):
    """XGBoost model for sports predictions"""
    
    def __init__(self, model_params: Dict = None):
        default_params = {
            'n_estimators': 200,
            'max_depth': 5,
            'learning_rate': 0.01,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'objective': 'multi:softprob',
            'num_class': 3,
            'eval_metric': 'mlogloss',
            'early_stopping_rounds': 20,
            'verbose': False,
        }
        
        # Update with user params
        if model_params:
            default_params.update(model_params)
        
        super().__init__("xgboost", default_params)
    
    def _build_model(self):
        """Build XGBoost model"""
        return xgb.XGBClassifier(**self.model_params)
    
    def train(self, X_train, y_train, X_val=None, y_val=None):
        """Train with early stopping if validation data provided"""
        if X_val is not None and y_val is not None:
            # Enable early stopping
            self.model = self._build_model()
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False
            )
        else:
            # Train without early stopping
            super().train(X_train, y_train)
