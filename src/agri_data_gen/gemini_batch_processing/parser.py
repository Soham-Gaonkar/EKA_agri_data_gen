
"""
Reads batch_requests.jsonl and raw_results.jsonl, extracts:
- 'prompt' and 'system_instruction' from batch_requests
- 'thoughts' and 'advisory' from raw_results
and writes a new JSONL,  and Parquet file with these fields per line.
"""


import json
import os
import re
import pandas as pd
import markdown
import pdfkit  

from agri_data_gen.gemini_batch_processing.create_pdf import create_docx, convert_to_pdf



def read_jsonl(filepath):
	with open(filepath, 'r', encoding='utf-8') as f:
		return [json.loads(line) for line in f if line.strip()]

def write_jsonl(filepath, data):
	with open(filepath, 'w', encoding='utf-8') as f:
		for item in data:
			f.write(json.dumps(item, ensure_ascii=False) + '\n')

def write_parquet(filepath, data):
	pd.DataFrame(data).to_parquet(filepath, index=False)

def extract_prompt(batch_request):
	req = batch_request.get('request', {})
	contents = req.get('contents', [])
	if contents and 'parts' in contents[0] and contents[0]['parts']:
		text = contents[0]['parts'][0].get('text', '')
		return text.strip()
	return ''

def extract_system_instruction(batch_request):
	req = batch_request.get('request', {})
	sys_inst = req.get('system_instruction', {})
	parts = sys_inst.get('parts', [])
	if parts:
		text = parts[0].get('text', '')
		return text.strip()
	return ''

def extract_thoughts(raw_result):
	candidates = raw_result.get('response', {}).get('candidates', [])
	for cand in candidates:
		parts = cand.get('content', {}).get('parts', [])
		for part in parts:
			if isinstance(part, dict) and part.get('thought'):
				return part.get('text', '').strip()
	# fallback
	if 'thoughts' in raw_result:
		return str(raw_result['thoughts'])
	return ''

def extract_advisory(raw_result):
	candidates = raw_result.get('response', {}).get('candidates', [])
	for cand in candidates:
		parts = cand.get('content', {}).get('parts', [])
		for part in parts:
			if isinstance(part, dict) and part.get('thought'):
				continue
			if isinstance(part, dict) and 'text' in part:
				text = part['text']
				if isinstance(text, bytes):
					try:
						text = text.decode('utf-8')
					except Exception:
						text = text.decode('latin-1', errors='replace')
				text = str(text).strip()
				if text:
					return text
	# fallback
	adv = raw_result.get('advisory')
	if adv:
		if isinstance(adv, list):
			return '\n'.join(str(a) for a in adv)
		if isinstance(adv, bytes):
			try:
				adv = adv.decode('utf-8')
			except Exception:
				adv = adv.decode('latin-1', errors='replace')
		return str(adv)
	return ''

def safe_str(val):
	if val is None:
		return ''
	if isinstance(val, (dict, list)):
		return json.dumps(val, ensure_ascii=False)
	return str(val)

def extract_fields(batch_requests, raw_results):
	"""Extract required fields from batch_requests and raw_results."""
	output = []
	for req, res in zip(batch_requests, raw_results):
		entry = {
			'custom_id': safe_str(req.get('custom_id')),
			'prompt': safe_str(extract_prompt(req)),
			'system_instruction': safe_str(extract_system_instruction(req)),
			'thoughts': safe_str(extract_thoughts(res)),
			'advisory': safe_str(extract_advisory(res))
		}
		output.append(entry)
	return output




def clean_and_parse_string(text):
    """
    Attempts to fix double-escaped strings or JSON strings.
    Example: "{\"Growth\": \"Stage\"}" -> "Growth: Stage"
    """
    if not text:
        return ""
    

    # 2. Check if it's actually valid JSON (common in Batch inputs)
    try:
        data = json.loads(text)
        # If it parsed into a dict (e.g. prompt variables), format it nicely
        if isinstance(data, dict):
            formatted_lines = []
            for k, v in data.items():
                formatted_lines.append(f"<b>{k}:</b> {v}")
            return "<br>".join(formatted_lines)
        # If it parsed into a list
        elif isinstance(data, list):
            return "<br>".join([str(x) for x in data])
        # If it was just a quoted string, return the inner string
        return str(data)
    except (json.JSONDecodeError, TypeError):
        # It's just regular text, but let's handle newlines for HTML
        return text.replace('\n', '<br>')

def clean_advisory_text(text):
    """
    Removes Markdown code blocks or JSON wrappers if the model output them.
    """
    if not text: return "N/A"
    
    if "```" in text:
        match = re.search(r"```(?:\w+)?\n?(.*?)```", text, re.DOTALL)
        if match:
            text = match.group(1)
            
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            # If it has an 'advisory' key, return that
            if 'advisory' in data:
                return str(data['advisory'])
            # Or just dump the whole dict if strictly JSON was requested
            return json.dumps(data, ensure_ascii=False, indent=2)
    except:
        pass
        
    return text

# --- Report Generation ---

def generate_styled_report(data, output_dir):
    """
    Uses Pandas to process text and generates a high-quality HTML report.
    """
    print("Processing data with Pandas...")
    
    # 1. Load into Pandas
    df = pd.DataFrame(data)

    # 2. Clean Advisory & Thoughts (Remove wrappers, fix escapes)
    df['advisory_clean'] = df['advisory'].apply(clean_advisory_text)
    df['thoughts_clean'] = df['thoughts'].apply(clean_advisory_text) # Similar logic applies
    
    # 3. Convert to HTML (Markdown Parsing)
    df['advisory_html'] = df['advisory_clean'].apply(lambda x: markdown.markdown(x))
    df['thoughts_html'] = df['thoughts_clean'].apply(lambda x: markdown.markdown(x))
    df['system_html'] = df['system_instruction'].apply(lambda x: markdown.markdown(x) if x else "N/A")

    # 4. Clean Prompt (Parse JSON-strings into nice Key-Value lists)
    # We use the smart parser here instead of just .replace
    df['prompt_html'] = df['prompt'].apply(clean_and_parse_string)

    # 5. Construct the HTML Document
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Agri-Advisory Report</title>
        <style>
            @import url('[https://fonts.googleapis.com/css2?family=Noto+Sans+Devanagari:wght@400;700&family=Noto+Sans+Gujarati:wght@400;700&family=Roboto:wght@300;400;700&display=swap](https://fonts.googleapis.com/css2?family=Noto+Sans+Devanagari:wght@400;700&family=Noto+Sans+Gujarati:wght@400;700&family=Roboto:wght@300;400;700&display=swap)');
            
            body { font-family: 'Noto Sans Devanagari', 'Noto Sans Gujarati', 'Roboto', sans-serif; background: #f4f6f8; padding: 20px; color: #333; }
            .container { max-width: 900px; margin: 0 auto; background: white; padding: 40px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            h1 { text-align: center; color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }
            
            .entry-card { border: 1px solid #e1e4e8; border-radius: 8px; margin-bottom: 40px; overflow: hidden; page-break-inside: avoid; }
            
            .meta-header { background: #2c3e50; padding: 10px 20px; font-size: 0.9em; color: #fff; display: flex; justify-content: space-between; }
            
            /* Section Styling */
            .section { padding: 15px 20px; border-bottom: 1px solid #eee; }
            .label { font-weight: bold; text-transform: uppercase; font-size: 0.7em; letter-spacing: 0.5px; display: block; margin-bottom: 5px; color: #555; }
            
            /* System Instruction - Purple Theme */
            .system-block { background: #f3f0ff; border-left: 4px solid #805ad5; }
            .system-block .label { color: #553c9a; }
            
            /* User Input - Grey Theme */
            .prompt-block { background: #f8f9fa; border-left: 4px solid #718096; }
            .prompt-block .content { font-family: 'Courier New', Courier, monospace; font-size: 0.9em; color: #2d3748; }

            /* Advisory - Green Theme */
            .advisory-block { background: #f0fff4; border-left: 4px solid #48bb78; }
            .advisory-block h2 { margin-top: 0; font-size: 1.1em; color: #22543d; }
            .advisory-block .label { color: #276749; }
            
            /* Thoughts - Orange/Yellow Theme */
            .thoughts-block { background: #fffaf0; border-left: 4px solid #ed8936; }
            .thoughts-block .label { color: #9c4221; }
            .thoughts-block { font-size: 0.9em; color: #744210; }

        </style>
    </head>
    <body>
        <div class="container">
            <h1>Agri-Advisory Batch Results</h1>
            <p style="text-align:center; color:#666;">Generated via Gemini Batch API</p>
    """

    for index, row in df.iterrows():
        html_content += f"""
        <div class="entry-card">
            <div class="meta-header">
                <span><strong>ID:</strong> {row['custom_id']}</span>
            </div>

            <div class="section system-block">
                <span class="label">System Role & Instructions:</span>
                {row['system_html']}
            </div>
            
            <div class="section prompt-block">
                <span class="label">User Input Context:</span>
                <div class="content">{row['prompt_html']}</div>
            </div>

            <div class="section advisory-block">
                <span class="label">Generated Advisory:</span>
                {row['advisory_html']}
            </div>
            
            <div class="section thoughts-block">
                <span class="label">Internal Reasoning (CoT):</span>
                {row['thoughts_html']}
            </div>
        </div>
        """

    html_content += "</div></body></html>"

    # Save HTML
    html_path = os.path.join(output_dir, 'parsed_results.html')
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"HTML Report generated: {html_path}")
    
    # Convert to PDF
    pdf_path = os.path.join(output_dir, 'parsed_results.pdf')
    try:
        print("Attempting to convert HTML to PDF via pdfkit...")
        pdfkit.from_file(html_path, pdf_path)
        print(f"- PDF Report generated: {pdf_path}")
    except OSError as e:
        print(f"- PDF Automation Skipped: {e}")
        print(f"- SOLUTION: Open '{html_path}' in Chrome/Safari and press Ctrl+P -> Save as PDF.")




def extract(raw_results_path, output_dir=None):
    if output_dir is None:
        output_dir = os.path.dirname(raw_results_path)
    
    os.makedirs(output_dir, exist_ok=True)

    batch_requests = read_jsonl(os.path.join(os.path.dirname(raw_results_path), 'batch_requests.jsonl'))
    raw_results = read_jsonl(raw_results_path)
	
    if len(batch_requests) != len(raw_results):
       print(f"Warning: batch_requests ({len(batch_requests)}) and raw_results ({len(raw_results)}) have different lengths.")
    
    parsed_data = extract_fields(batch_requests, raw_results)

    write_jsonl(os.path.join(output_dir, 'parsed_results.jsonl'), parsed_data)
    write_parquet(os.path.join(output_dir, 'parsed_results.parquet'), parsed_data)
    generate_styled_report(parsed_data, output_dir)
    print(f"Wrote {len(parsed_data)} records to {output_dir}, file:- {output_dir}/results.parquet ")


if __name__ == '__main__':
    path = "output/agri-advisory-job_1767082735/parsed_results-2.jsonl"
    INPUT_REL_PATH = "output/agri-advisory-job_1767082735/parsed_results.parquet"

    extract(path)
    # create_docx(INPUT_REL_PATH)
    # convert_to_pdf(path)

