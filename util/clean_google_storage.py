
import os
from google import genai
from dotenv import load_dotenv

load_dotenv()
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY_SOKET'))


print("\nCleaning up ALL files from Gemini...")
for f in client.files.list():
    try:
        client.files.delete(name=f.name)
        print(f"Deleted {f.name}")
    except Exception as e:
        print(f"Could not delete {f.name}: {e}")        
        
print("My files (after cleanup):")
for f in client.files.list():
    print(" ", f.name)