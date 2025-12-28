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
        
        # mix freely with everything
        self.INDEPENDENT_AXES = [
            "region",
            "growth_stage",
            "weather",
        ]
        self.DEPENDENT_GROUPS = ["crop"]
        
        # We will initialize adapters in load_all() once we have the schema
        self.adapters = {}

    def load_all(self):
        """
        Load datasets and taxonomy definitions.
        Then, initialize adapters with the correct schema attributes.
        """
        # Load Taxonomies 
        self.taxonomies = self.taxonomy_manager.get_active_taxonomies()
        all_groups = self.INDEPENDENT_AXES + self.DEPENDENT_GROUPS
        
        # Initialize Adapters dynamically based on loaded schemas
        print("Initializing adapters with schemas...")
        for group in all_groups:
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
        # axes_data = []
        independent_axes_data = []

        for group_name in self.INDEPENDENT_AXES:
            tax_def = next((t for t in self.taxonomies if t["group"] == group_name), None)
            
            if not tax_def:
                print(f"Warning: Taxonomy group '{group_name}' not found in DB. Skipping axis.")
                continue

            adapter = self.adapters.get(group_name)
            entries = tax_def["entries"]
            
            current_axis_values = []
            for entry in entries:
                if adapter:
                    real_data = adapter.sample(entry).get("data", entry)
                else:
                    real_data = entry
                current_axis_values.append((group_name, real_data))
            
            independent_axes_data.append(current_axis_values)

        # 2. Generate Combinations
        crop_def = next((t for t in self.taxonomies if t["group"] == "crop"), None)
        if not crop_def:
            raise ValueError("Crop taxonomy missing!")
        
        crop_adapter = self.adapters.get("crop")
        crop_entries = crop_def["entries"]

        # ====================================================
        # STEP 3: HYBRID GENERATION LOOP
        # ====================================================
        count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            
            # A. Loop through every combination of Independent Variables
            # (e.g., MP + Heavy Rain + Flowering)
            for indep_combo in itertools.product(*independent_axes_data):
                
                # Create the base context for this scenario
                base_bundle = {}
                for group_name, data in indep_combo:
                    base_bundle[group_name] = data

                # B. Loop through Crops
                for crop_entry in crop_entries:
                    # Process crop data via adapter
                    processed_crop = crop_adapter.sample(crop_entry).get("data", crop_entry)
                    
                    # Extract the nested problems list
                    # IMPORTANT: Use .get() to avoid errors if a crop has no problems listed
                    problems_list = processed_crop.get("problems", [])

                    if not problems_list:
                        continue

                    # Clean the crop object for the bundle
                    # We remove the full 'problems' list so it doesn't clutter the final JSON
                    crop_payload = processed_crop.copy()
                    if "problems" in crop_payload:
                        del crop_payload["problems"]

                    # C. Loop through the Specific Problems for this Crop
                    for problem in problems_list:
                        
                        # Clone the base bundle
                        final_bundle = base_bundle.copy()
                        final_bundle["id"] = count + 1
                        
                        # Add Crop and Specific Stress
                        final_bundle["crop"] = crop_payload
                        final_bundle["stress"] = problem # This contains the specific ID and Label
                        
                        # Write to file
                        f.write(json.dumps(final_bundle, ensure_ascii=False) + "\n")
                        count += 1

        print(f"Successfully generated {count} valid scenarios.")
        return str(output_path)









