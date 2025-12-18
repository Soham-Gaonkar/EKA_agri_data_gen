import os
import yaml
from pathlib import Path
from typing import Dict, List, Any
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


class TaxonomyManager:
    """
    Manages taxonomy schemas (dimensions), not data.
    Each taxonomy corresponds to exactly one dimension (group).
    """

    REQUIRED_KEYS = {"group", "attributes", "entries"}

    def __init__(self, db_name: str = "taxonomy_db", collection_name: str = "taxonomies"):
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise ValueError("MONGO_URI environment variable not set.")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # index for fast lookup by group
        self.collection.create_index("group", unique=True)

    def load_from_files_and_store(self, taxonomy_dir: str) -> None:
        """
        Load taxonomy YAML/JSON files and upsert them into MongoDB.
        One document per taxonomy (group).
        """

        taxonomy_paths = (
            list(Path(taxonomy_dir).glob("*.yaml")) +
            list(Path(taxonomy_dir).glob("*.yml")) +
            list(Path(taxonomy_dir).glob("*.json"))
        )

        if not taxonomy_paths:
            raise FileNotFoundError(f"No taxonomy files found in {taxonomy_dir}")

        for path in taxonomy_paths:
            taxonomy = self._load_taxonomy_file(path)
            self._validate_taxonomy_schema(taxonomy)

            taxonomy_doc = {
                "group": taxonomy["group"],
                "description": taxonomy.get("description", ""),
                "attributes": taxonomy["attributes"],
                "entries": taxonomy["entries"],
                "source_file": path.name,
                "active": True
            }

            # upsert by group (dimension name)
            self.collection.update_one(
                {"group": taxonomy_doc["group"]},
                {"$set": taxonomy_doc},
                upsert=True
            )

    def get_active_taxonomies(self) -> List[Dict[str, Any]]:
        """
        Returns all active taxonomy definitions.
        """
        return list(self.collection.find({"active": True}, {"_id": 0}))

    def get_taxonomy(self, group: str) -> Dict[str, Any]:
        """
        Fetch a taxonomy by group name.
        """
        taxonomy = self.collection.find_one(
            {"group": group, "active": True},
            {"_id": 0}
        )
        if not taxonomy:
            raise KeyError(f"Active taxonomy not found for group '{group}'")
        return taxonomy

    def get_entries(self, group: str) -> List[Dict[str, str]]:
        """
        Returns the list of entries for a taxonomy group.
        """
        taxonomy = self.get_taxonomy(group)
        return taxonomy["entries"]

    def get_attributes(self, group: str) -> List[str]:
        """
        Returns the attribute keys for a taxonomy group.
        """
        taxonomy = self.get_taxonomy(group)
        return taxonomy["attributes"]

    def _load_taxonomy_file(self, path: Path) -> Dict[str, Any]:
        if path.suffix in {".yaml", ".yml"}:
            return yaml.safe_load(path.read_text(encoding="utf-8"))
        elif path.suffix == ".json":
            import json
            return json.loads(path.read_text(encoding="utf-8"))
        else:
            raise ValueError(f"Unsupported taxonomy file type: {path}")

    def _validate_taxonomy_schema(self, taxonomy: Dict[str, Any]) -> None:
        missing = self.REQUIRED_KEYS - taxonomy.keys()
        if missing:
            raise ValueError(
                f"Taxonomy '{taxonomy.get('group', 'UNKNOWN')}' "
                f"is missing required keys: {missing}"
            )

        if not isinstance(taxonomy["attributes"], list):
            raise TypeError("taxonomy.attributes must be a list")

        if not isinstance(taxonomy["entries"], list):
            raise TypeError("taxonomy.entries must be a list")

        for entry in taxonomy["entries"]:
            if "id" not in entry or "label" not in entry:
                raise ValueError(
                    f"Each taxonomy entry must have 'id' and 'label': {entry}"
                )

                
    def reset_taxonomy_collection(self):
        """Deletes all taxonomy documents from MongoDB."""
        result = self.collection.delete_many({})
        return result.deleted_count
