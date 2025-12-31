import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

# Initialize Client
api_key = os.getenv("GEMINI_API_KEY") 
if not api_key:
    # Fallback to check other keys
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY_2")

client = genai.Client(api_key=api_key)

print("Fetching available models...\n")

try:
    # 1. Get the list of models
    models = client.models.list()
    
    for model in models:
        # 2. Extract the name (this attribute is consistent)
        name = model.name
        
        # 3. Safely inspect supported methods
        # The new SDK might store this in a dictionary or different attribute.
        # We try to get it from the raw model dictionary.
        
        # Convert Pydantic model to dict
        model_data = model.model_dump(exclude_none=True) 
        
        # Check for supported methods in various possible keys
        methods = model_data.get("supported_generation_methods", [])
        
        # Filter: We are looking for "batch" capability
        # Note: Sometimes "generateContent" is enough if the model is on the whitelist.
        # But explicitly looking for "batchGenerateContent" is safer.
        
        print(f"🔹 Model: {name}")
        print(f"   Methods: {methods}")
        
        if "batchGenerateContent" in methods:
             print("   ✅ SUPPORTED for Batch!")
        else:
             print("   ❌ Batch not explicitly listed (might still work if stable version)")
        
        print("-" * 30)

except Exception as e:
    print(f"An error occurred: {e}")

print("\nDone.")