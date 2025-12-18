import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

class PerplexityProvider:
    """
    Minimal wrapper around Perplexity Sonar models.
    Call:   PerplexityProvider().generate(prompt)
    """

    def __init__(self, model_name: str = "sonar-pro"):
        """
        Initializes the provider with a specific model.
        """
        self.api_key = os.getenv("PERPLEXITY_API_KEY")
        if not self.api_key:
            raise RuntimeError("Set PERPLEXITY_API_KEY.")
        
        self.api_url = "https://api.perplexity.ai/chat/completions"
        self.model_name = model_name
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def generate(self, prompt: str) -> str:
        """
        Sends a prompt to the Perplexity API and returns the text response.
        """
        payload = {
            "model": self.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an AI assistant."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }

        try:
            response = requests.post(self.api_url, headers=self.headers, json=payload)
            response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
            
            # Extract the content from the response
            resp_json = response.json()
            return resp_json['choices'][0]['message']['content'].strip()

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response body: {response.text}")
            return f"Error: {response.status_code}"
        except Exception as err:
            print(f"An other error occurred: {err}")
            return "Error: Could not get a response from the API."


if __name__ == '__main__':
    try:
        perplexity_client = PerplexityProvider(model_name="sonar-pro")
        my_prompt = "Generate a multiple-choice question about the 'de facto doctrine' in administrative law."
        generated_text = perplexity_client.generate(my_prompt)
        print(generated_text)

    except RuntimeError as e:
        print(e)