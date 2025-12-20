import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

from .base_adapter import BaseAdapter


class WeatherAdapter(BaseAdapter):
    """
    Adapter for weather.csv dataset.
    Maps taxonomy weather buckets to real sampled weather rows.
    Implements Option A thresholds for bucket classification.
    """

    def __init__(self, csv_path: str = "data/raw/weather.csv"):
        self.csv_path = Path(csv_path)
        self.df = None

    # LOAD DATASET
    def load(self):
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Weather dataset not found at: {self.csv_path}")

        self.df = pd.read_csv(self.csv_path)

        required_cols = {
            "temperature_celsius",
            "humidity",
            "precip_mm",
            "wind_kph",
            "location_name",
            "region"
        }

        if not required_cols.issubset(self.df.columns):
            raise ValueError(f"Weather dataset missing required columns: {required_cols}")

        # Normalize location names (important for generating stable entry IDs)
        self.df["location_id"] = self.df["location_name"].str.lower().str.replace(" ", "_")

    # TAXONOMY ENTRY IDS (weather buckets)
    def get_all_ids(self) -> List[str]:
        """
        Returns all weather bucket IDs the taxonomy defines.
        """
        return [
            "weather_hot_dry",
            "weather_hot_humid",
            "weather_cool_dry",
            "weather_cool_humid",
            "weather_heavy_rain",
            "weather_moderate",
            "weather_arid",
        ]

    # RULES FOR EACH BUCKET  (Option A)
    def _filter_bucket(self, bucket: str) -> pd.DataFrame:
        df = self.df

        # convenience vars
        temp = df["temperature_celsius"]
        hum = df["humidity"]
        rain = df["precip_mm"]

        # Thresholds
        HOT = temp > 30
        COOL = temp < 22
        HUMID = hum > 70
        DRY_HUMIDITY = hum < 40
        NO_RAIN = rain == 0
        LIGHT_RAIN = (rain > 0) & (rain <= 5)
        MODERATE_RAIN = (rain > 5) & (rain <= 20)
        HEAVY_RAIN = rain > 20

        # Bucket definitions 
        if bucket == "weather_hot_dry":
            return df[HOT & DRY_HUMIDITY & NO_RAIN]

        if bucket == "weather_hot_humid":
            return df[HOT & HUMID]

        if bucket == "weather_cool_dry":
            return df[COOL & NO_RAIN]

        if bucket == "weather_cool_humid":
            return df[COOL & HUMID]

        if bucket == "weather_heavy_rain":
            return df[HEAVY_RAIN]

        if bucket == "weather_moderate":
            return df[
                (~HOT & ~COOL) &
                (~HEAVY_RAIN) &
                (hum >= 40) & (hum <= 70)
            ]

        if bucket == "weather_arid":
            return df[HOT & DRY_HUMIDITY & NO_RAIN]

        # Should never reach here
        raise KeyError(f"Unknown weather bucket: {bucket}")

    # SAMPLING LOGIC
    def sample(self, entry_id: str) -> Dict[str, Any]:
        """
        Returns structured weather JSON consistent with taxonomy attributes.
        """

        if self.df is None:
            raise RuntimeError("Call load() before sample()")

        if entry_id not in self.get_all_ids():
            raise KeyError(f"Unknown weather entry ID: {entry_id}")

        bucket_df = self._filter_bucket(entry_id)

        if bucket_df.empty:
            # If no rows match, fallback to random row (but mark as 'approx')
            row = self.df.sample(1).iloc[0]
        else:
            row = bucket_df.sample(1).iloc[0]

        # return {
        #     "avg_temperature_c": float(row["temperature_celsius"]),
        #     "max_temperature_c": float(row["temperature_fahrenheit"]) * 0.556,   # approx conversion 
        #     "rainfall_mm": float(row["precip_mm"]),
        #     "humidity_percent": float(row["humidity"]),
        #     "wind_speed_kph": float(row["wind_kph"]),
        #     "location_name": row["location_name"],
        #     "region": row["region"],
        # }

        return {
            "avg_temperature_c": None,
            "max_temperature_c": None,   # approx conversion 
            "rainfall_mm": None,
            "humidity_percent": None,
            "wind_speed_kph": None,
            "location_name": None,
            "region": None,
        }

    # # WEATHER STRESS LABEL
    # def _infer_weather_stress(self, bucket: str) -> str:
    #     if bucket in ["weather_hot_dry", "weather_arid"]:
    #         return "heat_stress"
    #     if bucket == "weather_heavy_rain":
    #         return "flood_stress"
    #     if bucket in ["weather_cool_humid"]:
    #         return "fungal_risk"
    #     return "no_stress"
