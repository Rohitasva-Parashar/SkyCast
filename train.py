# train.py
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_absolute_error
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import joblib
from datetime import datetime


class EnhancedDelayPredictor:
    def __init__(self, data_path="flight_data_enhanced.xlsx"):
        self.df = pd.read_excel(data_path, parse_dates=[
            'scheduled_departure', 'actual_departure',
            'scheduled_arrival', 'actual_arrival'
        ])
        self.preprocess_data()

    def preprocess_data(self):
        # Feature engineering
        self.df['departure_delay'] = (
                                             self.df['actual_departure'] - self.df[
                                         'scheduled_departure']).dt.total_seconds() / 60
        self.df['arrival_delay'] = self.df['delay_minutes']
        self.df['is_delayed'] = (self.df['arrival_delay'] > 15).astype(int)

        # Temporal features
        self.df['departure_hour'] = self.df['scheduled_departure'].dt.hour
        self.df['departure_day'] = self.df['scheduled_departure'].dt.dayofweek
        self.df['departure_month'] = self.df['scheduled_departure'].dt.month
        self.df['flight_duration'] = (
                                             self.df['scheduled_arrival'] - self.df[
                                         'scheduled_departure']).dt.total_seconds() / 60

        # Encode categoricals
        self.encoders = {}
        for col in ['airline', 'origin', 'aircraft', 'route_weather']:
            le = LabelEncoder()
            self.df[col] = le.fit_transform(self.df[col].astype(str))
            self.encoders[col] = le

    def train_models(self):
        features = [
            'airline', 'origin', 'departure_hour', 'departure_day',
            'departure_month', 'flight_duration', 'route_weather',
            'route_temp', 'route_wind', 'departure_delay'
        ]

        X = self.df[features]
        y_class = self.df['is_delayed']
        y_reg = self.df['arrival_delay']

        # Classification model
        X_train, X_test, y_train, y_test = train_test_split(
            X, y_class, test_size=0.2, random_state=42)

        self.clf = RandomForestClassifier(n_estimators=200, max_depth=15)
        self.clf.fit(X_train, y_train)
        print(f"Classification Accuracy: {accuracy_score(y_test, self.clf.predict(X_test)):.3f}")

        # Regression model (only delayed flights)
        delayed = self.df[self.df['is_delayed'] == 1]
        X_reg = delayed[features]
        y_reg = delayed['arrival_delay']

        X_train, X_test, y_train, y_test = train_test_split(
            X_reg, y_reg, test_size=0.2, random_state=42)

        self.reg = RandomForestRegressor(n_estimators=200, max_depth=15)
        self.reg.fit(X_train, y_train)
        print(f"Regression MAE: {mean_absolute_error(y_test, self.reg.predict(X_test)):.1f} minutes")

    def save_models(self):
        joblib.dump(self.clf, 'enhanced_delay_clf.joblib')
        joblib.dump(self.reg, 'enhanced_delay_reg.joblib')
        joblib.dump(self.encoders, 'enhanced_encoders.joblib')


if __name__ == "__main__":
    predictor = EnhancedDelayPredictor()
    predictor.train_models()
    predictor.save_models()