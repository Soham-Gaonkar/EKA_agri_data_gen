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
            "growth_stage",
            "weather",
            "soil_type",
            "farming_practice"
        ]
        self.DEPENDENT_GROUPS = [
            "crop",
            "region_lang",
        ]
        
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


        # Generate Combinations
        #crop
        crop_def = next((t for t in self.taxonomies if t["group"] == "crop"), None)
        if not crop_def:
            raise ValueError("Crop taxonomy missing!")
        crop_adapter = self.adapters.get("crop")
        crop_entries = crop_def["entries"]

        #region
        region_def = next((t for t in self.taxonomies if t["group"] == "region_lang"), None)
        if not region_def:
            raise ValueError("Region_lang taxonomy missing!")
        region_adapter = self.adapters.get("region_lang")
        region_entries = region_def["entries"]

        #HYBRID GENERATION LOOP
        count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            
            #cartesian product (independent entries)
            for indep_combo in itertools.product(*independent_axes_data):
                context_bundle = {}
                for group_name, data in indep_combo:
                    context_bundle[group_name] = data

                # Region
                for region_entry in region_entries:
                    processed_region = region_adapter.sample(region_entry).get("data", region_entry)
                    allowed_languages = processed_region.get("languages", [])

                    if not allowed_languages: 
                        continue

                    region_payload = processed_region.copy()
                    if "languages" in region_payload:
                        del region_payload["languages"]

                    #Languages (Nested inside Region)
                    for lang_entry in allowed_languages:
                        
                        #region+Language
                        base_bundle = context_bundle.copy()
                        base_bundle["region"] = region_payload
                        base_bundle["language"] = lang_entry

                        #crops
                        for crop_entry in crop_entries:
                            processed_crop = crop_adapter.sample(crop_entry).get("data", crop_entry)
                            problems_list = processed_crop.get("problems", [])

                            if not problems_list:
                                continue

                            crop_payload = processed_crop.copy()
                            if "problems" in crop_payload:
                                del crop_payload["problems"]

                            for problem in problems_list:
                                # Clone the base bundle
                                final_bundle = base_bundle.copy()
                                final_bundle["id"] = count + 1
                                # Add Crop and Specific Stress
                                final_bundle["crop"] = crop_payload
                                final_bundle["stress"] = problem # This contains the specific ID and Label
                                
                                f.write(json.dumps(final_bundle, ensure_ascii=False) + "\n")
                                count += 1

        print(f"Successfully generated {count} valid scenarios.")
        return str(output_path)



