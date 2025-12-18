import json
from typing import Dict, Any


class PromptBuilder:
    """
    Builds prompts for generating Hindi agronomic reasoning examples.
    """

    @staticmethod
    def build(input_context: Dict[str, Any], record_id: int) -> str:
        """
        Convert the structured input into a complete LLM prompt.
        """

        prompt = f"""
                You are an expert agricultural advisor generating realistic Indian crop advisory cases.

                ### TASK
                Using the structured context provided below, generate **one** Hindi farmer advisory example.

                ### RULES
                - Output must be **exactly one JSON dictionary**, no arrays, no markdown.
                - All natural language (farmer query, thinking, advisory) must be **in Hindi**.
                - Numbers can be in English digits.
                - Use the crop and weather metadata exactly as provided.  
                - Add missing realistic fields (soil_type, planting window, fertilizer used etc.) if not given.
                - "model_thinking" must show **very detailed internal reasoning**, referencing every environmental factor.
                - "advisor_response" must be actionable, realistic, and specific to Indian agriculture.
                - If the combination of the data is not feasible then simply reason why and give advisory why not feasible.
                IMPORTANT OUTPUT RULES:
                - Output ONLY a single valid JSON object
                - Do NOT include trailing commas
                - Do NOT include any text outside JSON
                - Do NOT include markdown or explanations
                - Ensure JSON is strictly parseable by json.loads()
                
                ### STRUCTURED CONTEXT
                {input_context}

                ### OUTPUT FORMAT (must follow exactly)
                {{
                "id": {record_id},
                "crop_name": str,
                "weather_summary": str,
                "farmer_query": str,
                "model_thinking": str,
                "advisor_response": str,
                "category": str
                }}

                Output only the JSON dictionary, nothing else.
            """

        return prompt.strip()
