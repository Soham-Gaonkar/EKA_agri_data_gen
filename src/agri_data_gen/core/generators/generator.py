import os
import re
import json
from pathlib import Path
from typing import Dict, Any, List

from agri_data_gen.core.prompt.prompt_builder import PromptBuilder
from agri_data_gen.core.providers.gemini_provider import GeminiProvider


class GenerationEngine:
    """
    Generates Hindi agronomic reasoning data from bundles.
    Each bundle -> one JSONL output record.
    """

    def __init__(self, 
                 bundle_dir: str = "data/bundles",
                 out_file: str = "data/generated/data.jsonl"):
        self.bundle_dir = Path(bundle_dir)
        self.out_file = Path(out_file)
        self.provider = GeminiProvider()

        self.out_file.parent.mkdir(parents=True, exist_ok=True)

    def _load_bundles(self) -> List[Path]:
        if not self.bundle_dir.exists():
            raise FileNotFoundError(f"Bundle directory not found: {self.bundle_dir}")

        return sorted(self.bundle_dir.glob("*.json"))

    def _validate_json(self, text: str) -> Dict[str, Any]:
        """
        Attempts to fix common JSON issues from LLM output.
        """
        # Remove markdown blocks
        # text = re.sub(r"```.*?```", "", text, flags=re.S)

        # Extract first JSON object
        start = text.find("{")
        end = text.rfind("}") +1
        if start == -1 or end == 0:
            print("test: ", text)
            raise ValueError("No JSON object found in output")

        json_text = text[start:end]

        # Remove trailing commas before } or ]
        # json_text = re.sub(r",\s*([}\]])", r"\1", json_text)

        return json.loads(json_text)

    def generate_all(self, limit: int = None):
        bundles = self._load_bundles()
        if limit:
            bundles = bundles[:limit]

        print(f"Found {len(bundles)} bundles.")

        with self.out_file.open("w", encoding="utf-8") as f_out:
            for idx, bundle_path in enumerate(bundles, start=1):

                bundle = json.loads(bundle_path.read_text())
                
                crop = bundle["crop"]
                weather = bundle["weather"]

                # Insert structured attributes into JSON snippet for final template
                input_context = {
                        "crop": crop,
                        "weather": weather
                    }
                prompt = PromptBuilder.build( json.dumps(input_context, ensure_ascii=False, indent=2 ), idx)

                print(f"Generating record {idx} for {bundle_path.name} ...")
                response = self.provider.generate(prompt)
                print("response: ", response)
                record = self._validate_json(response)

                combined_record = {
                    "id": idx,
                    "input": input_context,
                    "output": record
                }

                f_out.write(json.dumps(combined_record, ensure_ascii=False, indent=2) + "\n")

        print(f"\nGeneration complete. Output saved to {self.out_file}")
