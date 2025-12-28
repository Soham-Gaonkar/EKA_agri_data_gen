import itertools
import json
import yaml
import logging
from pathlib import Path
from typing import List, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SystemInstructionBuilder:
    """
    Generates a massive dataset of unique System Instructions by performing
    a Cartesian Product of 4 modular prompt sections.
    """

    def __init__(self, taxonomy_dir: str = "sample_data/sys_instructions_taxonomy", output_dir: str = "data/sys_instructions"):
        self.taxonomy_dir = Path(taxonomy_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define the exact order of assembly
        self.SECTIONS = [
            "system_role.yaml",
            "language_instructions.yaml",
            "internal_analysis.yaml",
            "output_constraints.yaml"
        ]

    def load_yaml_entries(self, filename: str) -> List[Dict]:
        """Helper to load 'entries' from a YAML file."""
        path = self.taxonomy_dir / filename
        if not path.exists():
            logger.error(f"Critical: File not found {path}")
            raise FileNotFoundError(f"{path} missing.")
            
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        entries = data.get("entries", [])
        logger.info(f"Loaded {len(entries)} entries from {filename}")
        return entries

    def build_instructions(self, filename: str = "system_instructions.jsonl"):
        """
        Generates the Cartesian Product of all sections and writes to JSONL.
        """
        output_path = self.output_dir / filename
        logger.info(f"Building System Instructions into: {output_path}...")

        # 1. Load all data into lists - data_lists will be a list of lists: [[Roles...], [Langs...], [Analysis...], [outputs...]]
        data_lists = []
        for yaml_file in self.SECTIONS:
            data_lists.append(self.load_yaml_entries(yaml_file))

        # 2. Perform Cartesian Product
        count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            for combination in itertools.product(*data_lists):
                # combination is a tuple: (role_entry, lang_entry, analysis_entry, output_entry)
                
                role, lang, analysis, output = combination

                # 3. Construct the Full System Prompt
                full_text = (
                    f"{role['text'].strip()}\n\n"
                    f"{lang['text'].strip()}\n\n"
                    f"{analysis['text'].strip()}\n\n"
                    f"{output['text'].strip()}"
                )

                # 4. Construct Metadata ID
                combo_id = f"{role['id']}__{lang['id']}__{analysis['id']}__{output['id']}"

                record = {
                    "id": count+1, #combo_id,
                    "components": {
                        "role_id": role['id'],
                        "lang_id": lang['id'],
                        "analysis_id": analysis['id'],
                        "output_id": output['id']
                    },
                    "system_instruction": full_text
                }

                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1

        logger.info(f"Successfully generated {count} unique System Instructions at {output_path}")
        return str(output_path)

if __name__ == "__main__":
    builder = SystemInstructionBuilder()
    builder.build_instructions()