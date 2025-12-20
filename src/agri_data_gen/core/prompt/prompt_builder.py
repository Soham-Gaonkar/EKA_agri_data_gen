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
                Fill all of the fields provided with proper range and values and mark this as input. 
                And generate the output with respect to these input fields.
                In the output return all those values in the same format. 

                ### RULES
                - Output must be **exactly one JSON dictionary**, no arrays, no markdown.
                - All natural language (farmer query, thinking, advisory) must be **in Hindi**.
                - Numbers can be in English digits.
                - Use the crop and weather metadata exactly as provided. And return it as well with filled values you used in exact same structure! (This is a must!) 
                - Add missing realistic fields (soil_type, planting window, fertilizer used etc.) if not given.
                - "model_thinking" must show **very detailed internal reasoning**, referencing every environmental factor.
                - "advisor_response" must be actionable, realistic, and specific to Indian agriculture.
                - If the combination of the data is not feasible then simply reason why and give advisory why not feasible.
                - the thinking should be highly elaborated and advisory should be shorter than thinking with proper advisory.

                ### STRUCTURED CONTEXT
                {input_context}

                ### OUTPUT FORMAT (must follow exactly)
                {{
                "farmer_query": str,
                "model_thinking": str,
                "advisor_response": str,
                }}

                Output only the JSON dictionary, nothing else.
            """

        return prompt.strip()
