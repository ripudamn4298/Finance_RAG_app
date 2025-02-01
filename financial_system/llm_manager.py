from huggingface_hub import InferenceClient
import json
import time
from typing import Optional, Dict, Any
from datetime import datetime
import streamlit as st
import re

class LLMManager:
    """Manages LLM operations using HuggingFace Inference API"""
    
    def __init__(self,  max_retries: int = 3, retry_delay: int = 3):
        # Initialize with HuggingFace API token from either parameter or Streamlit secrets
        self.api_token = st.secrets["huggingface"]["key"]
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.model = "mistralai/Mistral-7B-Instruct-v0.2"
        
        # Initialize InferenceClient
        self.client = InferenceClient(
            model=self.model,
            token=self.api_token,
            timeout=120
        )
        
        # Track call history
        self.call_history = []

    def _extract_json(self, text: str) -> str:
        """Extract JSON object from text, handling various formats"""
        # First try to find JSON between curly braces
        try:
            # Find the first { and last }
            json_start = text.find('{')
            json_end = text.rfind('}') + 1
            
            if json_start != -1 and json_end != -1:
                json_str = text[json_start:json_end]
                # Validate it's proper JSON
                json.loads(json_str)
                return json_str
        except json.JSONDecodeError:
            pass

        # If that fails, try more aggressive cleaning
        try:
            # Remove any markdown code block syntax
            cleaned = text.replace('```json', '').replace('```', '').strip()
            # Remove any explanatory text before the JSON
            if '{' in cleaned:
                cleaned = cleaned[cleaned.find('{'):cleaned.rfind('}')+1]
                # Validate it's proper JSON
                json.loads(cleaned)
                return cleaned
        except json.JSONDecodeError:
            pass

        # If all cleaning attempts fail, return original text
        return text
    
    def _log_call(self, prompt: str, response: str, duration: float, success: bool, error: Optional[str] = None):
        """Log details of each LLM call"""
        self.call_history.append({
            "timestamp": datetime.now().isoformat(),
            "prompt": prompt,
            "response": response,
            "duration": duration,
            "success": success,
            "error": error,
            "model": self.model
        })

    def generate_response(self, prompt: str, max_tokens: int = 500) -> str:
        """Generate response using HuggingFace Inference API"""
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                # Use text_generation instead of post
                response = self.client.text_generation(
                    prompt,
                    max_new_tokens=max_tokens,
                    temperature=0.7,
                    top_p=0.95,
                    do_sample=True
                )
                
                generated_text = response
                duration = time.time() - start_time
                
                # Check if the response is meant to be JSON
                if 'json' in prompt.lower() or '{' in generated_text:
                    generated_text = self._extract_json(generated_text)
                
                # Log successful call
                self._log_call(prompt, generated_text, duration, True)
                return generated_text.strip()
                
            except Exception as e:
                duration = time.time() - start_time
                error_msg = f"Attempt {attempt + 1} failed: {str(e)}"
                print(error_msg)
                
                # Log failed attempt
                self._log_call(prompt, "", duration, False, error_msg)
                
                # If not last attempt, wait before retrying
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    return f"Error: Unable to generate response after {self.max_retries} attempts"
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics from call history"""
        if not self.call_history:
            return {}
            
        total_calls = len(self.call_history)
        successful_calls = sum(1 for call in self.call_history if call["success"])
        avg_duration = sum(call["duration"] for call in self.call_history) / total_calls
        
        return {
            "total_calls": total_calls,
            "success_rate": (successful_calls / total_calls) * 100,
            "average_duration": avg_duration,
            "error_rate": ((total_calls - successful_calls) / total_calls) * 100
        }
    
    def __call__(self, prompt: str) -> str:
        """Make the class callable with just the prompt"""
        return self.generate_response(prompt)