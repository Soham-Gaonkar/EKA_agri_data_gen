import json
import os
import math
import time
import logging
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class BatchValidator:
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY_SOKET")
        self.model_name = "gemini-2.5-flash"
        self.client = genai.Client(api_key=self.api_key)

        self.BATCH_SIZE = 7700  # As requested
        self.job_id_base = f"val_job_{int(time.time())}"
        # self.job_id = f"validation_batch_{int(time.time())}"

        self.input_path = "data/bundles/bundles_hi_(-8401_16800).jsonl"
        self.batch_dir = "data/bundles/classify"
        os.makedirs(self.batch_dir, exist_ok=True)
        
        self.valid_output = f"{self.batch_dir}/valid_bundles.jsonl"
        self.weak_valid_output = f"{self.batch_dir}/weak_valid_bundles.jsonl"
        self.invalid_output = f"{self.batch_dir}/invalid_bundles.jsonl"

        self.system_instruction = """
        You are an Expert Agricultural Scientist.
        Your task is to evaluate whether the given agricultural scenario is biologically plausible.
        Evaluate based on: 1. Crop–Stress compatibility 2. Growth stage relevance 3. Weather suitability 4. Regional feasibility
        Return ONLY a confidence score between 0 and 1:
            - 0.80 to 1.0 = Fully plausible
            - 0.50 to 0.80 = Common but not ideal
            - 0.30 to 0.50 = Rare but biologically possible
            - 0.0 to 0.30 = Biologically implausible
            
        IMPORTANT:
        - Weather or stage mismatch alone does NOT automatically invalidate.
        - Rare but possible cases must be marked WEAK_VALID.
        - Farming practice NEVER invalidates a scenario.
        - Output format MUST be:
        {
        "<BUNDLE_ID>": {
        "confidence": <float>
        } }
        """

    def initialize_output_files(self):
        """Clears/Creates output files at the start so we can append later."""
        logger.info("Initializing output files (clearing previous run data)...")
        open(self.valid_output, 'w').close()
        open(self.weak_valid_output, 'w').close()
        open(self.invalid_output, 'w').close()


    # CREATE BATCH FILE
    def create_batch_files(self):
        """Reads big input file and splits it into multiple small request files."""
        if not os.path.exists(self.input_path):
            logger.error(f"Input file not found: {self.input_path}")
            return False

        logger.info("Reading input bundles...")

        with open(self.input_path, "r", encoding="utf-8") as f:
            all_bundles = [json.loads(line) for line in f if line.strip()]

        total_bundles = len(all_bundles)
        total_batches = math.ceil(total_bundles / self.BATCH_SIZE)
        
        logger.info(f"Loaded {total_bundles} bundles. Splitting into {total_batches} batches of {self.BATCH_SIZE}.")

        generated_files = []


        for i in range(total_batches):
            start_idx = i * self.BATCH_SIZE # i=batch number
            end_idx = start_idx + self.BATCH_SIZE
            chunk = all_bundles[start_idx:end_idx]
            
            # Create a unique filename for this batch
            batch_filename = f"{self.batch_dir}/{self.job_id_base}_req_part_{i+1}.jsonl"

            with open(batch_filename, "w", encoding="utf-8") as f_out:
                for bundle in chunk:
                    scenario_text = (
                        f"Bundle ID: {bundle['id']}\n"
                        f"Crop: {bundle['crop']['label']}\n"
                        f"Stage: {bundle['growth_stage']['label']}\n"
                        f"Weather: {bundle['weather']['label']}\n"
                        f"Stress: {bundle['stress']['label']}\n"
                        f"Soil Type: {bundle['soil_type']['label']}\n"
                        f"Region: {bundle['region']['label']}\n"
                        f"Farming Practice: {bundle['farming_practice']['label']}"
                    )

                    prompt = f"""Analyze the scientific plausibility of the following agricultural scenario: {scenario_text}"""

                    request_entry = {
                        "custom_id": str(bundle["id"]),
                        "request": {
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {
                                "responseMimeType": "application/json",
                                "temperature": 0.0,
                                "thinkingConfig": {
                                    "includeThoughts": False,
                                    "thinkingBudget": 0
                                }
                            },
                            "systemInstruction": {
                                "parts": [{"text": self.system_instruction}]
                            }
                        }
                    }

                    f_out.write(json.dumps(request_entry, ensure_ascii=False) + "\n")

            generated_files.append(batch_filename)
            logger.info(f"Created batch file {i+1}/{total_batches}: {batch_filename} ({len(chunk)} items)")

        return generated_files, all_bundles


    # SUBMIT & WAIT
    def process_single_batch(self, request_file, batch_index, total_batches):
        """Submits one batch file, waits, and downloads results."""
        logger.info(f"--- Processing Batch {batch_index}/{total_batches} ---")
        
        logger.info(f"Uploading {request_file}...")
        try:
            batch_input_file = self.client.files.upload(
                file=request_file,
                config=types.UploadFileConfig(
                    display_name=f"part_{batch_index}",
                    mime_type="text/plain"
                )
            )
        except Exception as e:
            logger.error(f"Upload failed for batch {batch_index}: {e}")
            return None

        logger.info(f"Submitting Job for Batch {batch_index}...")

        try:
            job = self.client.batches.create(
                model=self.model_name,
                src=batch_input_file.name,
                config={"display_name": f"{self.job_id_base}_part_{batch_index}"},
            )

        except Exception as e:
            logger.error(f"Job creation failed for batch {batch_index}: {e}")
            return None

        logger.info(f"Job {job.name} started. Waiting for completion...")


        while True:
            try:
                job = self.client.batches.get(name=job.name)

                if job.state.name == "JOB_STATE_SUCCEEDED":
                    logger.info(f"Batch {batch_index} COMPLETED.")
                    break
                elif job.state.name in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
                    logger.error(f"Batch {batch_index} FAILED with status: {job.state.name}")
                    if hasattr(job, 'error') and job.error:
                        logger.error(f"Error details: {job.error.message}")
                    return False

                logger.info(f"Status: {job.state.name}... waiting 180s")
                time.sleep(180)

            except Exception as e:
                logger.warning(f"Polling error (retrying): {e}")
                time.sleep(60)


        result_filename = request_file.replace("_req_", "_res_")

        try:
            content = self.client.files.download(file=job.dest.file_name)
            with open(result_filename, "wb") as f:
                f.write(content)
            logger.info(f"Results downloaded to: {result_filename}")
            return result_filename
        except Exception as e:
            logger.error(f"Failed to download results for batch {batch_index}: {e}")
            return None



    # PARSE & SPLIT RESULTS
    def parse_and_append_results(self, result_file, original_bundles_map):
        """Parses a single result file and appends to the main output files."""
        if not result_file or not os.path.exists(result_file):
            logger.warning("No result file to parse. Skipping.")
            return

        logger.info(f"Parsing and appending data from {result_file}...")
        

        validation_map = {}

        with open(result_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    resp = json.loads(line)
                    # Handle cases where model might error on specific item
                    if "response" in resp and "candidates" in resp["response"]:
                        text = resp["response"]["candidates"][0]["content"]["parts"][0]["text"]
                        parsed = json.loads(text)
                        for k, v in parsed.items():
                            validation_map[str(k)] = v
                except Exception:
                    continue

        logger.info(f"Loaded {len(validation_map)} validation responses.")

        valid_cnt, weak_cnt, invalid_cnt, missed_cnt = 0, 0, 0, 0

        with open(self.valid_output, "a", encoding="utf-8") as valid_out, \
             open(self.weak_valid_output, "a", encoding="utf-8") as weak_out, \
             open(self.invalid_output, "a", encoding="utf-8") as invalid_out:

            for bid, decision in validation_map.items():
                bundle = original_bundles_map.get(bid)
                if not bundle:
                    continue # Should not happen if map is correct


                confidence = decision.get("confidence", 0.0)

                try:
                    confidence = float(confidence)
                except (ValueError, TypeError):
                    confidence = 0.0

                if confidence >= 0.50:
                    label = "VALID"
                elif confidence >= 0.30:
                    label = "WEAK_VALID"
                else:
                    label = "INVALID"

                bundle["validation_confidence"] = confidence
                bundle["validation_label"] = label

                if label == "VALID":
                    valid_out.write(json.dumps(bundle, ensure_ascii=False) + "\n")
                    valid_cnt += 1
                elif label == "WEAK_VALID":
                    weak_out.write(json.dumps(bundle, ensure_ascii=False) + "\n")
                    weak_cnt += 1
                else:
                    bundle["validation_status"] = "INVALID"
                    invalid_out.write(json.dumps(bundle, ensure_ascii=False) + "\n")
                    invalid_cnt += 1

        print("Batch Stats:")
        print(f"Valid        : {valid_cnt}")
        print(f"Weak_valid   : {weak_cnt}")
        print(f"Invalid      : {invalid_cnt}")



    def run_pipeline(self):
        self.initialize_output_files()
        request_files, all_bundles = self.create_batch_files()
        if not request_files:
            return

        # Key: ID, Value: Bundle 
        bundles_map = {str(b['id']): b for b in all_bundles}
        
        # Sequential process 
        total = len(request_files)
        for i, req_file in enumerate(request_files):
            res_file = self.process_single_batch(req_file, i+1, total)
            if res_file:
                self.parse_and_append_results(res_file, bundles_map)
            else:
                logger.error(f"Skipping parsing for batch {i+1} due to failure.")

        logger.info("ALL BATCHES PROCESSED.")


if __name__ == "__main__":
    validator = BatchValidator()
    validator.run_pipeline()




