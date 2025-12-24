import os
import time
import json
import threading
import concurrent.futures
from pathlib import Path
from typing import Dict, Any, List
from tqdm import tqdm 

from agri_data_gen.core.prompt.prompt_builder import PromptBuilder
from agri_data_gen.core.providers.gemini_provider import GeminiProvider


class RateLimiter:
    """
    Manages API rate limits (RPM) locally to prevent 429 errors.
    """
    def __init__(self, max_calls_per_minute: int = 10):
        self.delay = 60.0 / max_calls_per_minute
        self.last_call = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_call = time.time()

class GenerationEngine:
    """
    Optimized Engine for JSONL Bundles.
    Features: Parallel Processing, Rate Limiting, Crash Recovery.
    """

    def __init__(self, 
                 bundle_file: str = "data/bundles/bundles.jsonl",
                 out_file: str = "data/generated/data.jsonl",
                 max_workers: int = 1,  # Adjust based on API tier
                 rpm_limit: int = 10):  
        
        self.bundle_file = Path(bundle_file)
        self.out_file = Path(out_file)
        self.provider = GeminiProvider()
        self.max_workers = max_workers
        
        # Ensure output directory exists
        self.out_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Thread-safe writing
        self.file_lock = threading.Lock()
        
        # Rate Limiter
        self.limiter = RateLimiter(max_calls_per_minute=rpm_limit)
        print(f"Output will be saved to: {self.out_file.absolute()}")

    def _load_processed_ids(self) -> set:
        """
        Reads the output file to check which IDs are already done.
        Allows resuming if the script stops.
        """
        processed_ids = set()
        if self.out_file.exists():
            with open(self.out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        # Assuming the input bundle has a 'bundle_id' or we use index
                        if "bundle_id" in record:
                            processed_ids.add(record["bundle_id"])
                        # elif "id" in record:
                        #     processed_ids.add(record["id"])
                    except json.JSONDecodeError:
                        continue
        return processed_ids

    def _process_single_bundle(self, line: str, line_idx: int):
        """
        Worker function to process one line of JSONL.
        """
        try:
            bundle = json.loads(line)
            #  This ID determines resume capability. 
            bundle_id = bundle.get("bundle_id", f"row_{line_idx}")

            # Input Construction
            input_context = bundle # Pass everything (Crop, Weather, etc.)
            
            # Prompt Building
            prompt = PromptBuilder.build(
                json.dumps(input_context, ensure_ascii=False, indent=2), 
                bundle_id
            )

            # Rate Limiting 
            self.limiter.wait()

            # API Call with Retry Logic
            response = self._call_provider_with_retry(prompt)

            # Result Construction
            combined_record = {
                "id": line_idx,
                # "bundle_id": bundle_id,
                # "input": input_context,
                "output": response 
            }

            # Thread-Safe Write with FLUSH
            with self.file_lock:
                with open(self.out_file, "a", encoding="utf-8") as f_out:
                    f_out.write(json.dumps(combined_record, ensure_ascii=False) + "\n")
                    f_out.flush() 
                    os.fsync(f_out.fileno())

            return True

        except Exception as e:
            print(f"Error processing row {line_idx}: {e}")
            return False


    def _call_provider_with_retry(self, prompt, retries=3):
        """
        Handles 429 (Rate Limit) and 500 errors with exponential backoff.
        """
        base_delay = 10
        for attempt in range(retries):
            try:
                # Assuming provider.generate returns the string text
                return self.provider.generate(prompt)
            except Exception as e:
                error_msg = str(e).lower()
                # Check for rate limit or server errors
                if "429" in error_msg or "quota" in error_msg or "500" in error_msg:
                    wait_time = base_delay * (2 ** attempt)
                    print(f"API Limit hit. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                elif "500" in error_msg or "internal" in error_msg:
                    print(f"⚠️ Server Error (500). Retrying...")
                    time.sleep(2)
                else:
                    raise e # Raise other errors immediately
        raise Exception("Max retries exceeded")


    def generate_all(self, limit: int = None):
        """
        Main execution loop using ThreadPool.
        """
        print(f"Starting Generation Engine")

        # Load existing work to skip
        processed_ids = self._load_processed_ids()
        print(f"Found {len(processed_ids)} already processed records. Skipping them.")

        # Read all lines 
        with open(self.bundle_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        if limit:
            lines = lines[:limit]

        # Filter out already done
        work_items = []
        for idx, line in enumerate(lines, start=1):
            # Quick check if we can parse ID to skip
            try:
                b_id = json.loads(line).get("bundle_id", f"row_{idx}")
                if b_id not in processed_ids:
                    work_items.append((line, idx))
            except:
                continue
        
        if not work_items:
            print("All items already processed!")
            return
        
        #  Execution - for parallel increase executors.
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = [
                executor.submit(self._process_single_bundle, item[0], item[1]) 
                for item in work_items
            ]
            
            # Progress Bar
            for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), unit="req"):
                pass

        print(f"\nGeneration complete. Data saved to {self.out_file}")





if __name__ == "__main__":
    engine = GenerationEngine(
        bundle_file="data/bundles/all_scenarios.jsonl",
        rpm_limit=13, 
        max_workers=1
    )
    engine.generate_all()





