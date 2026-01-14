# scripts/quick_start.py
import pandas as pd
from src.models.xgboost_model import XGBoostModel

# Usa dati di esempio
data = {
    'feature1': [1, 2, 3, 4, 5],
    'feature2': [2, 3, 4, 5, 6],
    'target': [0, 1, 0, 1, 0]
}

df = pd.DataFrame(data)
X = df[['feature1', 'feature2']]
y = df['target']

model = XGBoostModel()
model.train(X, y)
print("Model trained!")
