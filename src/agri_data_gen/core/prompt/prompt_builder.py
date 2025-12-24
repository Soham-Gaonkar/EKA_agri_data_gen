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
                Role: Expert Agricultural Advisor (Kisan Mitra).
                Language: Hindi (Strictly).
                
                Input Data:
                ```json
                {input_context}
                ```
                
                Task:
                You are provided with a data bundle describing a specific agricultural scenario.
                
                Step 1: Feasibility Analysis (Crucial)
                Compare the 'Crop' requirements (Temperature, Rainfall, etc.) against the provided 'Weather' conditions and various other constraints.
                
                Step 2: Generate Advisory
                Based on Step 1, generate the advisory in Hindi:
                - If the scenario is IMPOSSIBLE/FATAL: 
                * Clearly state that farming this crop is NOT recommended.
                * Explain *why* clearly.
                * Do NOT give false hope or generic fertilizer tips for a dying crop.
                - If the scenario is STRESSFUL but SALVAGEABLE: 
                * Acknowledge the stress (e.g., "Drought stress", etc).
                * Provide specific mitigation steps.
                - If the scenario is IDEAL: 
                * Focus on yield maximization and standard care.

                Constraints:
                - Output strictly the advisory in hindi with proper utilisation of hindi words even for english terms.
                - Use simple, clear Hindi suitable for farmers. Use bullet points for steps.
                - Reference specific numbers from the input (e.g., "Since rainfall is 0mm...", etc).
                """

        return prompt.strip()
