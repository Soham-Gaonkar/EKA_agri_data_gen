import time
import sys
import json
import os
# import yaml
import itertools
import random
import glob
import logging
from google import genai
from google.genai import types
from dotenv import load_dotenv
from pathlib import Path

from agri_data_gen.gemini_batch_processing.parser import extract


load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TextBatchJob:
    def __init__(self, job_name="agri-advisory-job", api_key=None):
        
        self.api_key = api_key if api_key else os.getenv('GOOGLE_API_KEY_SOKET')
        
        self.model_name = "models/gemini-2.5-flash"
        self.client = genai.Client(api_key=self.api_key)  
        self.job_name = job_name
        self.job_base_id = f"{job_name}_{int(time.time())}"
        self.output_dir = f"output/{self.job_base_id}"
        self.sys_instructions = self.load_system_instructions("data/sys_instructions/system_instructions.jsonl")
        
        self.MAX_TOKENS_PER_BATCH = 2_500_000  # Safe Limit
        self.EST_TOKENS_PER_REQ = 650         
        self.MAX_REQS_PER_BATCH = self.MAX_TOKENS_PER_BATCH // self.EST_TOKENS_PER_REQ 
        
        self.cursor_file = "data/cursor.txt"  #state file

    def prepare_prompt(self, data_bundle):
        """
        Converts the data bundle into a clean, natural language string 
        suitable for PDF rendering and easier LLM reading.
        """
        lines = []
        for k, v in data_bundle.items():
            if isinstance(v, dict) and "label" in v:
                key = k.replace("_", " ").title()
                value = v["label"]
                lines.append(f"{key}: {value}")
        
        return "\n".join(lines)


    def load_system_instructions(self, filepath):
        """Loads all system instruction objects from the JSONL file into a list."""
        instructions = []
        if not os.path.exists(filepath):
            logger.error(f"System instruction file not found: {filepath}")
            raise FileNotFoundError(f"{filepath} not found.")
            
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    instructions.append(json.loads(line))
        logger.info(f"Loaded {len(instructions)} system instructions.")
        return instructions

    def get_random_system_instruction(self):
        """Picks one random instruction object from the loaded list."""
        if not self.sys_instructions:
             raise ValueError("System instructions list is empty!")
        
        selected_obj = random.choice(self.sys_instructions)
        return selected_obj['system_instruction']



    def _get_start_index(self):
        """Reads the last processed index from the cursor file."""
        if not os.path.exists(self.cursor_file):
            return 0
        try:
            with open(self.cursor_file, 'r') as f:
                return int(f.read().strip())
        except ValueError:
            return 0

    def _update_cursor(self, new_index):
        """Updates the cursor file with the new index."""
        with open(self.cursor_file, 'w') as f:
            f.write(str(new_index))


    def create_jsonl_batches(self, input_file_path: str= "data/bundles/bundles.jsonl"):
        """
        Reads input bundles using the Cursor Method.
        formatted Batch API requests to the output JSONL file.
        """
        start_index = self._get_start_index()

        if not os.path.exists(input_file_path):
            logger.error("Input file not found.")
            return False

        os.makedirs(self.output_dir, exist_ok=True) 

        logger.info(f"Reading from {input_file_path}...")
        
        created_files = []
        current_batch_reqs = []
        total_processed = 0
        batch_counter = 1
        
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            
            # 2. Efficiently Skip 'start_index' lines (skip already processed lines)
            iterator = itertools.islice(infile, start_index, None)

            for line in iterator:
                try:
                    bundle = json.loads(line.strip())
                    total_processed += 1

                    # Logic: Generate Request Object
                    custom_id = bundle.get('id', f"req_{start_index + total_processed}")
                    # prompt_text = json.dumps(self.prepare_prompt(bundle)) #,  ensure_ascii=False) #json dump to parse into string - save token 
                    prompt_text = self.prepare_prompt(bundle)
                    sys_instruction_text = self.get_random_system_instruction()

                    request_entry = {
                        "custom_id": str(custom_id), 
                        "request": { 
                            "contents": [{"parts": [{"text": prompt_text}]} ],
                            "system_instruction": { "parts": [{"text": sys_instruction_text}] },
                            "generationConfig": {
                                "responseMimeType": "text/plain", 
                                "temperature": 0.2,
                                "thinkingConfig": { 
                                    "includeThoughts": True,
                                    "thinkingBudget": 2048
                                },
                            }
                        }
                    }

                    current_batch_reqs.append(request_entry)
                    
                    # Check if batch is full
                    if len(current_batch_reqs) >= self.MAX_REQS_PER_BATCH:
                        batch_filename = f"{self.output_dir}/batch_part_{batch_counter:03d}.jsonl"
                        self._write_batch_file(batch_filename, current_batch_reqs)
                        created_files.append(batch_filename)
                        
                        # Reset for next batch
                        current_batch_reqs = []
                        batch_counter += 1

                except json.JSONDecodeError:
                    continue

            # Write remaining requests if any
            if current_batch_reqs:
                batch_filename = f"{self.output_dir}/batch_part_{batch_counter:03d}.jsonl"
                self._write_batch_file(batch_filename, current_batch_reqs)
                created_files.append(batch_filename)
        
        if total_processed == 0:
            logger.info("No new records found. The job is complete!")
            return None 

        logger.info(f"Created {len(created_files)} batch files containing {total_processed} requests.")
        return created_files, (start_index + total_processed)


    def _write_batch_file(self, filename, requests):
        with open(filename, 'w', encoding='utf-8') as f:
            for req in requests:
                f.write(json.dumps(req, ensure_ascii=False) + "\n")
        logger.info(f"Saved: {filename} ({len(requests)} items)")



    def submit_wait_download(self, file_path):
        """Uploads the JSONL and starts the Batch Job"""
        logger.info(f"--- Processing {os.path.basename(file_path)} ---")
        logger.info("Uploading JSONL file to Gemini...")
        batch_input_file = self.client.files.upload(
            file=file_path,
            config=types.UploadFileConfig(display_name=Path(file_path).stem, mime_type="text/plain")
        )
        
        logger.info(f"File uploaded: {batch_input_file.name}. Starting Batch Job...")
        logger.info("Submitting Job...")

        job = self.client.batches.create( 
            model=self.model_name,
            src=batch_input_file.name,
            config={
                'display_name': f"{self.job_base_id}_{Path(file_path).stem}"
            },
        )
        
        logger.info(f"Batch Job Created: {job.name}")


        #wait
        logger.info("Waiting for completion...")
        while True:
            job_status = self.client.batches.get(name=job.name)
            state = job_status.state.name 
            logger.info(f"Job Status: {state}")
            
            if state == "JOB_STATE_SUCCEEDED":
                break
            elif state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
                logger.error("Job Failed.")
                return False
        
            time.sleep(180) 
            print("waiting for 180")

        #download
        output_file_name = job_status.dest.file_name
        logger.info(f"Downloading results from {output_file_name}...")

        content = self.client.files.download(file=output_file_name)

        input_path = Path(file_path)
        # Create 'output' folder inside the input directory
        results_dir = input_path.parent / "output"
        results_dir.mkdir(exist_ok=True)

        #Construct new path: parent_dir/output/filename_results.jsonl
        res_filename = f"{input_path.stem}_results.jsonl"
        res_path = results_dir / res_filename

        with open(res_path, 'wb') as f:
            f.write(content)
            
        # Parse and Separate Files
        # extract(str(res_path))
        logger.info(f"Batch Complete. Results at {res_path}")
        print(res_path)
        return True




if __name__ == "__main__":
    processor = TextBatchJob()
    
    result = processor.create_jsonl_batches("data/bundles/bundles.jsonl")
    
    if result is None:
        print("No new data to process.")
        sys.exit(0)
    
    # Now it is safe to unpack
    files, final_cursor_pos = result
    
    print(f"Plan: Process {len(files)} batch files sequentially.")
    
    # Process Sequentially
    success_count = 0
    for f in files:
        if processor.submit_wait_download(f):
            success_count += 1
            time.sleep(30)
        else:
            logger.error(f"Failed to process {f}. Stopping pipeline.")
            break
            
    # Update Cursor
    if success_count == len(files):
        processor._update_cursor(final_cursor_pos)
        print("All batches processed successfully! Cursor updated.")
    else:
        print(f"Pipeline stopped. Processed {success_count}/{len(files)} batches.")