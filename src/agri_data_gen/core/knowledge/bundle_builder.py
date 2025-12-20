import json
import itertools
from typing import List, Dict, Any
from pathlib import Path

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
        self.taxonomy_manager = TaxonomyManager()

        # Register Adapters Dynamically
        self.adapters = {
            "crop": CropAdapter(),
            "weather": WeatherAdapter(),
            # "soil": SoilAdapter(),  
            # "pest": PestAdapter(),
        }

    def load_all(self):
        """
        Load datasets and taxonomy definitions.
        Must be called before building.
        """
        for adapter in self.adapters.values():
            if hasattr(adapter, 'load'):
                adapter.load()

        # fetch taxonomies (MongoDB → list of dicts)
        self.taxonomies = self.taxonomy_manager.get_active_taxonomies()


    def _get_taxonomy(self, group_name: str) -> Dict[str, Any]:
        """
        Returns taxonomy dict for the specified group name.
        """
        for t in self.taxonomies:
            if t["group"] == group_name:
                return t
        raise KeyError(f"Taxonomy group '{group_name}' not found in DB.")


    def build_all(self, filename: str = "bundles.jsonl") -> str:
        """
        Core function:
        - Iterates crop entries
        - Iterates weather entries
        - Samples dataset values
        - Writes structured bundles directly to a JSONL file.
        
        Returns:
            The path to the generated JSONL file.
        """
        output_path = self.out_dir / filename
        print(f"Building bundles into: {output_path} ...")

        # Prepare "Axes" for the Cartesian Product
        axes_data = []
        sorted_taxonomies = sorted(self.taxonomies, key=lambda x: x['group'])

        # Generate the list of values for this specific axis (dimension)
        for tax_def in sorted_taxonomies:
            group_name = tax_def["group"]
            entries = tax_def["entries"]
            
            # Check if we have an adapter for this group
            adapter = self.adapters.get(group_name)
            if not adapter:
                print(f"Warning: No adapter found for taxonomy group '{group_name}'. Skipping.")
                continue

            # Generate the list of values for this specific axis (dimension)
            current_axis_values = []
            for entry in entries:
                entry_id = entry["id"]
                # Use the adapter to get the real numbers/data
                real_data = adapter.sample(entry_id)
                
                # Store as a tuple: (Group Name, ID, Real Data) 
                current_axis_values.append((group_name, entry_id, real_data))
            
            if current_axis_values:
                axes_data.append(current_axis_values)


        count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            # itertools.product(*axes_data) automatically creates every combination
            for combination in itertools.product(*axes_data):
                # 'combination' is a tuple of the items we packed above.
                # Example: (('crop', 'maize', {...}), ('weather', 'hot', {...}), ('soil', 'clay', {...}))
                
                bundle = {}
                id_parts = []

                for group_name, entry_id, real_data in combination:
                    bundle[group_name] = real_data
                    id_parts.append(entry_id)

                # Create the unique ID
                bundle["bundle_id"] = "__".join(id_parts)

                f.write(json.dumps(bundle, ensure_ascii=False) + "\n")
                count += 1

        print(f"Successfully generated {count} unique scenarios.")
        return str(output_path)