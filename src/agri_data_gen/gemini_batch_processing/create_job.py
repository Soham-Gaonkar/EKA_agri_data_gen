import time
import json
import os
import yaml
import itertools
import random
import logging
from google import genai
from google.genai import types
from dotenv import load_dotenv

from agri_data_gen.gemini_batch_processing.parser import extract


load_dotenv()
# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TextBatchJob:
    def __init__(self, job_name="agri-advisory-job", batch_size=10000):
        self.api_key = os.getenv('GOOGLE_API_KEY_SOKET')
        self.model_name = "models/gemini-2.5-flash"
        self.client = genai.Client(api_key=self.api_key)  
        self.job_name = job_name
        self.job_id = f"{job_name}_{int(time.time())}"
        self.output_dir = f"output/{self.job_id}"
        self.jsonl_path = f"{self.output_dir}/batch_requests.jsonl"
        self.sys_instructions = self.load_system_instructions("data/sys_instructions/system_instructions.jsonl")
        self.batch_size = batch_size    
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


    def create_jsonl(self, input_file_path: str= "data/bundles/bundles.jsonl"):
        """
        Reads input bundles using the Cursor Method.
        formatted Batch API requests to the output JSONL file.
        """
        start_index = self._get_start_index()
        logger.info(f"Cursor found at index: {start_index}. Processing next {self.batch_size} records.")

        if not os.path.exists(input_file_path):
            logger.error("Input file not found.")
            return False

        os.makedirs(self.output_dir, exist_ok=True) # Create output dir now

        logger.info(f"Reading from {input_file_path}...")
        logger.info(f"Writing batch requests to {self.jsonl_path}...")
        request_count = 0
        lines_processed_in_this_run = 0
        
        with open(input_file_path, 'r', encoding='utf-8') as infile, \
                    open(self.jsonl_path, 'w', encoding='utf-8') as outfile:
            
            # 2. Efficiently Skip 'start_index' lines 
            # islice(iterator, start, stop) -> skips to 'start', stops at 'stop'
            # stopping at start_index + self.batch_size creates the exact chunk we need
            batch_iterator = itertools.islice(infile, start_index, start_index + self.batch_size)

            for line in batch_iterator:
                lines_processed_in_this_run += 1
                try:
                    bundle = json.loads(line.strip())
                    custom_id = bundle.get('id', f"req_{start_index + lines_processed_in_this_run}")

                    prompt_text = json.dumps(self.prepare_prompt(bundle),  ensure_ascii=False) #json dump to parse into string - save token 
                    sys_instruction_text = self.get_random_system_instruction()

                    # Construct Request Object 
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

                    #write to batch file
                    outfile.write(json.dumps(request_entry, ensure_ascii=False) + "\n")
                    request_count += 1
                    
                except json.JSONDecodeError:
                    logger.error(f"Skipping invalid JSON at line ")
                    continue
        
        if request_count == 0:
            logger.info("No new records found. The job is complete!")
            return False # Signal that there is no work to do

        # Update Cursor ONLY if requests were written successfully
        new_cursor = start_index + lines_processed_in_this_run
        self._update_cursor(new_cursor)
        
        logger.info(f"Successfully created batch file with {request_count} requests.")
        logger.info(f"Cursor updated to {new_cursor}.")
        return True 


    def submit_job(self):
        """Uploads the JSONL and starts the Batch Job"""
        logger.info("Uploading JSONL file to Gemini...")
        batch_input_file = self.client.files.upload(
            file=self.jsonl_path,
            config=types.UploadFileConfig(display_name="my-batch-requests", mime_type="text/plain") 
        )
        
        logger.info(f"File uploaded: {batch_input_file.name}. Starting Batch Job...")
        
        self.batch_job = self.client.batches.create( 
            model=self.model_name,
            src=batch_input_file.name,
            config={
                'display_name': self.job_id,
            },
        )
        
        logger.info(f"Batch Job Created: {self.batch_job.name}")
        return self.batch_job


    def wait_for_completion(self):
        """Polls the job status"""
        while True:
            job_status = self.client.batches.get(name=self.batch_job.name)
            state = job_status.state.name 
            logger.info(f"Job Status: {state}")
            
            if state in ["JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
                return job_status
            
            time.sleep(180) 
            print("waiting for 120")


    def download_and_parse_results(self):
        """Downloads the result file and parses the outputs"""
        job = self.client.batches.get(name=self.batch_job.name)
        
        if job.state.name != "JOB_STATE_SUCCEEDED":
            logger.error("Job failed or incomplete.") 
            return 

        # Check both locations just to be safe across SDK versions

        output_file_name = job.dest.file_name
        
        logger.info(f"Downloading results from {output_file_name}...")
        
        # Download raw content
        content = self.client.files.download(file=output_file_name)

        # Save Raw Output
        os.makedirs(self.output_dir, exist_ok=True)
        raw_path = f"{self.output_dir}/raw_results.jsonl"
        with open(raw_path, 'wb') as f:
            f.write(content)
            
        # Parse and Separate Files
        extract(raw_path)
        print(raw_path)
        return raw_path



if __name__ == "__main__":

    processor = TextBatchJob(batch_size=10000)
    
    processor.create_jsonl("data/bundles/bundles.jsonl")
    
    processor.submit_job()
    
    processor.wait_for_completion()
    
    raw_path = processor.download_and_parse_results()
    print("Saved raw result at: ", raw_path)






