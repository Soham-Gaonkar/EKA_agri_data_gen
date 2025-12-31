import os
from google import genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# 1. Setup Client
api_key = os.getenv('GOOGLE_API_KEY_SOKET') 
if not api_key: raise ValueError("API Key not found")

client = genai.Client(api_key=api_key)

print(f"{'FILE ID':<20} | {'DISPLAY NAME':<30} | {'SIZE (MB)':<10} | {'STATE'}")
print("-" * 80)

total_bytes = 0
file_count = 0

# 2. List all files and sum usage
try:
    for f in client.files.list():
        # Convert size to MB
        size_mb = f.size_bytes / (1024 * 1024)
        total_bytes += f.size_bytes
        file_count += 1
        
        # Clean up display name (handle None)
        display_name = f.display_name if f.display_name else "(no name)"
        
        # Print row
        print(f"{f.name.split('/')[-1]:<20} | {display_name[:28]:<30} | {size_mb:.2f} MB   | {f.state.name}")

except Exception as e:
    print(f"Error listing files: {e}")

# 3. Summary Calculation
print("-" * 80)
total_mb = total_bytes / (1024 * 1024)
hard_limit_mb = 20000.0 # 20GB is the fixed limit for all projects

print(f"\nSTORAGE SUMMARY:")
print(f"   • Total Files:   {file_count}")
print(f"   • Used Storage:  {total_mb:.2f} MB")
print(f"   • Max Limit:     {hard_limit_mb:,.0f} MB (Fixed Platform Limit)")
print(f"   • Remaining:     {hard_limit_mb - total_mb:.2f} MB")

# Alert if full
if total_mb > 18000:
    print("\nWARNING: You are running out of space! Run cleanup_storage.py soon.")