import os
from google.generativeai import GenerativeModel, configure
from dotenv import load_dotenv


load_dotenv()

class GeminiProvider:
    """
    Minimal wrapper around Gemini 2.5 Flash.
    Call:   GeminiProvider().generate(prompt)
    """

    def __init__(self, model_name: str = "models/gemini-2.5-flash"):
        api_key = os.getenv("GOOGLE_API_KEY_2")
        if not api_key:
            raise RuntimeError("Set GOOGLE_API_KEY env var first.")
        configure(api_key=api_key)
        self.model = GenerativeModel(model_name)

    def generate(self, prompt: str) -> str:
        resp = self.model.generate_content(prompt)
        return resp.text.strip()
