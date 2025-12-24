import json
import itertools
from typing import List, Dict, Any
from pathlib import Path

from agri_data_gen.core.data_access.taxonomy_manager import TaxonomyManager
from agri_data_gen.core.data_access.adapters.adapter import GenericAdapter

class BundleBuilder:
    """
    Generates structured bundles based on strict hierarchical order:
    Region -> Crop -> Classification -> Variety -> Stress -> Yield -> Stage
    """

    def __init__(self, out_dir: str = "data/bundles"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.taxonomy_manager = TaxonomyManager()
        
        # 1. Define the Strict Order
        self.ORDER = [
            "region",
            "crop",
            "classification",
            # "variety",
            # "stress",
            # "yield_potential",
            # "growth_stage"
        ]
        
        # We will initialize adapters in load_all() once we have the schema
        self.adapters = {}

    def load_all(self):
        """
        Load datasets and taxonomy definitions.
        Then, initialize adapters with the correct schema attributes.
        """
        # 1. Load Taxonomies first
        self.taxonomies = self.taxonomy_manager.get_active_taxonomies()
        
        # 2. Initialize Adapters dynamically based on loaded schemas
        print("Initializing adapters with schemas...")
        for group in self.ORDER:
            # Find the taxonomy definition to get its attributes
            tax_def = next((t for t in self.taxonomies if t["group"] == group), None)
            
            # If found, extract attributes (e.g. ['soil_type', 'rainfall'])
            attrs = tax_def["attributes"] if tax_def else []
            
            # Initialize the adapter with these attributes
            self.adapters[group] = GenericAdapter(group, attributes=attrs)
            self.adapters[group].load() # Validates readiness

    def build_all(self, filename: str = "bundles.jsonl") -> str:
        output_path = self.out_dir / filename
        print(f"Building ordered bundles into: {output_path} ...")

        # 1. Collect Data for each Axis in strict order
        axes_data = []
        
        for group_name in self.ORDER:
            tax_def = next((t for t in self.taxonomies if t["group"] == group_name), None)
            
            if not tax_def:
                print(f"Warning: Taxonomy group '{group_name}' not found in DB. Skipping axis.")
                continue

            adapter = self.adapters.get(group_name)
            entries = tax_def["entries"]
            
            current_axis_values = []
            for entry in entries:
                entry_id = entry["id"]
                
                # FIX 2: Pass the FULL entry object, not just ID
                if adapter:
                    # Returns {'data': {...}, 'schema_attributes': [...]}
                    real_data = adapter.sample(entry) 
                else:
                    real_data = {"data": entry} # Fallback structure
                
                current_axis_values.append((group_name, entry_id, real_data))
            
            if current_axis_values:
                axes_data.append(current_axis_values)

        # 2. Generate Combinations
        count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            for combination in itertools.product(*axes_data):
                bundle = {}
                id_parts = []

                for group_name, entry_id, real_data in combination:
                    # Unpack the structured data from the adapter
                    # We store the actual data (id, label, attributes) in the bundle
                    bundle[group_name] = real_data.get("data", real_data)
                    
                    id_parts.append(entry_id)

                # Create ID
                bundle["bundle_id"] = "__".join(id_parts)

                f.write(json.dumps(bundle, ensure_ascii=False) + "\n")
                count += 1

        print(f"Successfully generated {count} unique scenarios.")
        return str(output_path)