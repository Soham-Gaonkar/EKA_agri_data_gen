import logging
from typing import Dict, Any, List, Optional

# Setup logger for production visibility
logger = logging.getLogger(__name__)

class GenericAdapter:
    """
    A robust, schema-aware adapter that standardizes taxonomy entries.
    
    Role:
    1. Validates that entries match the required attribute schema.
    2. Packages data cleanly for the downstream Prompt Builder.
    3. Handles missing or malformed data gracefully without crashing the pipeline.
    """

    def __init__(self, group_name: str, attributes: List[str] = None):
        """
        Args:
            group_name: The taxonomy group (e.g., "crop", "region").
            attributes: A list of expected keys (schema) from the taxonomy YAML.
        """
        self.group_name = group_name
        # Ensure attributes is never None to prevent iteration errors
        self.attributes = attributes or []
        
        # Internal cache or state if needed later (e.g. for validation stats)
        self._processed_count = 0

    def load(self):
        """
        Lifecycle hook. 
        In a GenericAdapter, we don't load external CSVs/DBs, 
        but we log readiness to aid debugging.
        """
        logger.info(f"[{self.group_name}] Adapter ready. Schema expects: {self.attributes}")

    def sample(self, entry_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Takes a raw entry from YAML and standardizes it.
        
        Args:
            entry_data: The dictionary from the 'entries' list in YAML.
            
        Returns:
            A clean dictionary containing:
            - 'entry': The raw data values
            - 'meta': Schema info (useful for the LLM prompt construction)
        """
        self._processed_count += 1
        
        # 1. Validation (Soft): Check if crucial attributes are missing
        # We don't crash, but we warn. This helps you spot bad YAML data.
        missing_keys = [attr for attr in self.attributes if attr not in entry_data]
        if missing_keys:
            logger.warning(
                f"[{self.group_name}] Entry '{entry_data.get('id', 'unknown')}' "
                f"is missing expected attributes: {missing_keys}"
            )

        # 2. Cleanup: Ensure 'label' exists (critical for LLM human-readability)
        if "label" not in entry_data:
            entry_data["label"] = entry_data.get("id", "Unknown").replace("_", " ").title()

        # 3. Structure for the Prompt Builder
        # We wrap it so the PromptBuilder knows specifically *what* to ask the LLM to fill.
        return {
            # The actual data (e.g., id="reg_gujarat", soil="Black")
            "data": entry_data,
            
            # The schema (e.g., ["soil_type", "rainfall"])
            # The PromptBuilder will use this list to instruct the LLM: 
            # "For this region, ensure you consider these specific attributes..."
            "schema_attributes": self.attributes,
            
            # Metadata for tracing
            "adapter_type": "generic",
            "group": self.group_name
        }