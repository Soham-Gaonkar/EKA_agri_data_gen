from pathlib import Path
from typing import List, Dict, Any
import json

from agri_data_gen.core.data_access.taxonomy_manager import TaxonomyManager
from agri_data_gen.core.data_access.adapters.crop_adapter import CropAdapter
from agri_data_gen.core.data_access.adapters.weather_adapter import WeatherAdapter


class BundleBuilder:
    """
    Generates structured bundles of:
        crop × weather
        
    No prompt mixing. No LLM calls.
    Produces clean JSON bundles to be consumed later by the generator.
    """

    def __init__(self, out_dir: str = "data/bundles"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        # dataset adapters
        self.crop_adapter = CropAdapter()
        self.weather_adapter = WeatherAdapter()

        # taxonomy access
        self.taxonomy_manager = TaxonomyManager()

    # INITIALIZE
    def load_all(self):
        """
        Load datasets and taxonomy definitions.
        Must be called before building.
        """
        self.crop_adapter.load()
        self.weather_adapter.load()

        # fetch taxonomies (MongoDB → list of dicts)
        self.taxonomies = self.taxonomy_manager.get_active_taxonomies()

        # extract groups cleanly
        self.crop_taxonomy = self._get_taxonomy("crop")
        self.weather_taxonomy = self._get_taxonomy("weather")

    def _get_taxonomy(self, group_name: str) -> Dict[str, Any]:
        """
        Returns taxonomy dict for the specified group name.
        """
        for t in self.taxonomies:
            if t["group"] == group_name:
                return t
        raise KeyError(f"Taxonomy group '{group_name}' not found in DB.")

    def build_all(self) -> List[Dict[str, Any]]:
        """
        Core function:
        - Iterates crop entries
        - Iterates weather entries
        - Samples dataset values
        - Produces structured bundles
        """

        bundles = []

        for crop_entry in self.crop_taxonomy["entries"]:
            crop_id = crop_entry["id"]
            crop_data = self.crop_adapter.sample(crop_id)

            for weather_entry in self.weather_taxonomy["entries"]:
                weather_id = weather_entry["id"]
                weather_data = self.weather_adapter.sample(weather_id)

                bundle = {
                    "bundle_id": f"{crop_id}__{weather_id}",
                    "crop": crop_data,
                    "weather": weather_data,
                }

                # save bundle as JSON
                out_path = self.out_dir / f"{bundle['bundle_id']}.json"
                out_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False))

                bundles.append(bundle)

        return bundles

