import os
from google import genai
from dotenv import load_dotenv


load_dotenv()

class GeminiProvider:
    """
    Minimal wrapper around Gemini 2.5 Flash.
    Call:   GeminiProvider().generate(prompt)
    """

    def __init__(self, model_name: str = "models/gemini-2.5-flash"):
        api_key = os.getenv("GOOGLE_API_KEY_3")
        if not api_key:
            raise RuntimeError("Set GOOGLE_API_KEY env var first.")
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name
        
    def generate(self, prompt: str) -> str:
        response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt
                )        
        return reresponsesp.text.strip()
