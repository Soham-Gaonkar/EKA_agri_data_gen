import time
import json
import os
import yaml
import random
import logging
from google import genai
from google.genai import types
from dotenv import load_dotenv


load_dotenv()
# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TextBatchJob:
    def __init__(self, job_name="agri-advisory-job"):
        self.api_key = os.getenv('GOOGLE_API_KEY_SOKET')
        self.model_name = "models/gemini-2.5-flash"
        self.client = genai.Client(api_key=self.api_key)  
        self.job_name = job_name
        self.job_id = f"{job_name}_{int(time.time())}"
        self.output_dir = f"output/{self.job_id}"
        os.makedirs(self.output_dir, exist_ok=True)
        self.jsonl_path = f"{self.output_dir}/batch_requests.jsonl"
        self.sys_instructions = self.load_system_instructions("data/sys_instructions/system_instructions.jsonl")

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
        target_lang = "Hindi"
        final_instruction = selected_obj['system_instruction'].replace("{target_language}", target_lang)

        # Returns just the text content of the instruction
        return final_instruction


    def create_jsonl(self, input_file_path: str= "data/bundles/bundles.jsonl"):
        """
        Reads input bundles from a JSONL file line-by-line and writes 
        formatted Batch API requests to the output JSONL file.
        This allows processing massive datasets without memory issues.
        """

        logger.info(f"Reading from {input_file_path}...")
        logger.info(f"Writing batch requests to {self.jsonl_path}...")
        request_count = 0
        
        with open(input_file_path, 'r', encoding='utf-8') as infile, \
                    open(self.jsonl_path, 'w', encoding='utf-8') as outfile:
            
            for index, line in enumerate(infile):
                try:
                    bundle = json.loads(line.strip())
                    custom_id = bundle.get('bundle_id', f"req_{index}")

                    prompt_text = json.dumps(self.prepare_prompt(bundle),  ensure_ascii=False) #json dump to parse into string - save token 
                    sys_instruction_text = self.get_random_system_instruction()

                    # Construct Request Object 
                    request_entry = {
                        "custom_id": custom_id, 
                        "request": { 
                            "contents": [{"parts": [{"text": prompt_text}]} ],
                            "system_instruction": { "parts": [{"text": sys_instruction_text}] },
                            "generationConfig": {
                                "responseMimeType": "application/json", 
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
                    logger.error(f"Skipping invalid JSON at line {index}")
                    continue
        
        logger.info(f"Successfully created batch file with {request_count} requests.")


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
            
            time.sleep(60) 
            print("waiting for 60")


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
        raw_path = f"{self.output_dir}/raw_results.jsonl"
        with open(raw_path, 'wb') as f:
            f.write(content)
            
        # Parse and Separate Files
        # self.parse_raw_results(raw_path)
        return raw_path


    # def parse_raw_results(self, raw_path):
    #     logger.info("Parsing results...")
    #     with open(raw_path, 'r', encoding='utf-8') as f:
    #         for line in f:
    #             try:
    #                 response_item = json.loads(line)
    #                 custom_id = response_item.get("custom_id", "unknown_id")
                    
    #                 if "response" in response_item:
    #                     candidates = response_item["response"].get("candidates", [])
    #                     if not candidates: continue

    #                     parts = candidates[0]["content"]["parts"]
    #                     thinking_text = ""
    #                     advisory_text = ""

    #                     # Extract Thought vs Answer
    #                     for part in parts:
    #                         if part.get("thought") is True:
    #                             thinking_text += part.get("text", "")
    #                         else:
    #                             advisory_text += part.get("text", "")

    #                     # Clean up Advisory JSON
    #                     try:
    #                         final_advisory = json.loads(advisory_text)
    #                     except:
    #                         final_advisory = {"raw_text": advisory_text}

    #                     # Save Final Clean File
    #                     final_output = {
    #                         "id": custom_id,
    #                         "thinking": thinking_text,
    #                         "advisory": final_advisory
    #                     }
                        
    #                     out_file = f"{self.output_dir}/{custom_id}.json"
    #                     with open(out_file, "w", encoding="utf-8") as out:
    #                         json.dump(final_output, out, indent=2, ensure_ascii=False)
                            
    #             except Exception as e:
    #                 logger.error(f"Error parsing line: {e}")


if __name__ == "__main__":

    processor = TextBatchJob()
    
    processor.create_jsonl("data/bundles/bundles.jsonl")
    
    processor.submit_job()
    
    processor.wait_for_completion()
    
    raw_path = processor.download_and_parse_results()
    print("Saved raw result at: ", raw_path)





