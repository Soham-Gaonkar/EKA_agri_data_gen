from typing import Dict
from agri_data_gen.core.data_access.adapters.crop_adapter import CropAdapter
from agri_data_gen.core.data_access.adapters.weather_adapter import WeatherAdapter


class AdapterRegistry:
    """
    Central registry. Allows BundleBuilder to dynamically
    pull sampler objects for each taxonomy group.
    """

    def __init__(self):
        self.adapters: Dict[str, object] = {}

    def load_all(self):
        """
        Initialize adapters for groups we support.
        Add soil_adapter, symptom_adapter etc. later.
        """
        self.adapters = {
            "crop": CropAdapter("data/raw/Crop_recommendation.csv"),
            "weather": WeatherAdapter("data/raw/weather.csv")
        }

        # Load all datasets now (important)
        for adapter in self.adapters.values():
            adapter.load()

    def get_adapter(self, group: str):
        if group not in self.adapters:
            raise KeyError(f"No adapter registered for group '{group}'")
        return self.adapters[group]
