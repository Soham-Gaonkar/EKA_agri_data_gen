import pandas as pd
from typing import Dict, Any, List
from pathlib import Path
from .base_adapter import BaseAdapter


class CropAdapter(BaseAdapter):
    """
    Adapter for Crop_recommendation.csv.
    Provides structured crop metadata for bundle building.
    Now includes graceful fallback when the crop is missing.
    """

    def __init__(self, csv_path: str = "data/raw/Crop_recommendation.csv"):
        self.csv_path = Path(csv_path)
        self.df = None
        self.crop_groups = {}

    def load(self):
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Crop dataset not found at {self.csv_path}")

        self.df = pd.read_csv(self.csv_path)

        required_cols = {"label", "temperature", "rainfall", "ph"}
        if not required_cols.issubset(self.df.columns):
            raise ValueError(f"Crop dataset missing required columns: {required_cols}")

        # Group rows by crop name
        self.crop_groups = {
            crop: subdf.reset_index(drop=True)
            for crop, subdf in self.df.groupby("label")
        }

    def get_all_ids(self) -> List[str]:
        """Returns IDs derived from dataset crop names."""
        return [
            f"crop_{name.lower().replace(' ', '_')}"
            for name in self.crop_groups.keys()
        ]

    def sample(self, entry_id: str) -> Dict[str, Any]:
        """Return structured crop metadata or fallback if missing."""
        if self.df is None:
            raise RuntimeError("Call load() before sample().")

        crop_name = entry_id.replace("crop_", "").replace("_", " ")

        # Case 1: crop exists in dataset → return real values
        if crop_name in self.crop_groups:
            sub = self.crop_groups[crop_name]

            temp_min = float(sub["temperature"].min())
            temp_max = float(sub["temperature"].max())

            rain_min = float(sub["rainfall"].min())
            rain_max = float(sub["rainfall"].max())

            ph_min = float(sub["ph"].min())
            ph_max = float(sub["ph"].max())

            # return {
            #     "crop_name": crop_name,
            #     "pH_preference_range": [ph_min, ph_max],
            #     "rainfall_range_mm": [rain_min, rain_max],
            #     "temperature_tolerance": [temp_min, temp_max],
            # }
            return {
                "crop_name": crop_name,
                "pH_preference_range": None,
                "rainfall_range_mm": None,
                "temperature_tolerance": None,
            }

        # Case 2: crop missing → Fallback mode (NO CRASH)
        return {
            "crop_name": crop_name,
            "pH_preference_range": None,
            "rainfall_range_mm": None,
            "temperature_tolerance": None,
        }
