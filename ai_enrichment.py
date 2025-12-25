import os
import json
import logging
from typing import Dict, List, Optional
import httpx
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentResult:
    """Structure for AI enrichment output"""
    skills: List[str]
    persona: str
    confidence_score: float
    reasoning: str
    raw_response: str


class PromptManager:
    """Manages AI prompts with config-driven approach"""
    
    def __init__(self, config_path: str = 'prompts_config.json'):
        self.config_path = config_path
        self.prompts = self.load_prompts()
    
    def load_prompts(self) -> Dict:
        """Load prompts from config file"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                return json.load(f)
        else:
            # Default prompts
            default_prompts = {
                "system_context": """You are an AI assistant helping the CMT Association (Chartered Market Technician) 
analyze volunteer member profiles. The CMT Association is a global credentialing body for technical analysts 
and market professionals. They need to match volunteers with opportunities based on skills and readiness.""",
                
                "enrichment_prompt": """Analyze this member profile and extract structured information:

Member Bio/Comment: {bio}

Provide your analysis in STRICT JSON format with these exact fields:
{{
  "skills": [list of technical/professional skills mentioned or implied],
  "persona": "one of: Mentor Material | Needs Guidance | Passive | Active Learner | Expert Contributor",
  "confidence_score": 0-100 integer based on clarity and detail of bio,
  "reasoning": "brief explanation of persona classification"
}}

Skills should include: programming languages (Python, R, etc.), financial domains (derivatives, options, algo trading, etc.), 
technical tools (pandas, numpy, machine learning, etc.), and soft skills (mentoring, teaching, etc.).

Persona Definitions:
- Mentor Material: Experienced, offers to help, has mentored before
- Needs Guidance: Beginner, struggling, explicitly asks for help
- Passive: Minimal engagement, vague interest
- Active Learner: Enthusiastic, actively learning, engaged
- Expert Contributor: Advanced skills, built systems, research background

Respond ONLY with valid JSON, no other text."""
            }
            
            # Save default prompts
            with open(self.config_path, 'w') as f:
                json.dump(default_prompts, f, indent=2)
            
            return default_prompts
    
    def get_enrichment_prompt(self, bio: str) -> str:
        """Generate enrichment prompt for a bio"""
        return self.prompts["enrichment_prompt"].format(bio=bio)
    
    def get_system_context(self) -> str:
        """Get system context prompt"""
        return self.prompts["system_context"]


class AIEnricher:
    """Handles AI-based enrichment of member data"""
    
    def __init__(self, api_key: str = None, model_name: str = None):
        """
        Initialize AI enricher
        
        Args:
            api_key: OpenRouter API key (or set OPENROUTER_API_KEY env var)
            model_name: Model to use (defaults to openai/gpt-4o-mini if None)
        """
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        if not self.api_key:
            raise ValueError("API key required. Set OPENROUTER_API_KEY env var or pass api_key parameter")
        
        # Default to a good, cost-effective model
        self.model_name = model_name or "openai/gpt-4o-mini"
        
        # OpenRouter API endpoint
        self.api_url = "https://openrouter.ai/api/v1/chat/completions"
        
        # HTTP client
        self.client = httpx.Client(
            timeout=60.0,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/cmt-volunteer-system",  # Optional: for analytics
                "X-Title": "CMT Volunteer System"  # Optional: for analytics
            }
        )
        
        self.prompt_manager = PromptManager()
        
        logger.info(f"Initialized AIEnricher with model: {self.model_name}")
        
        # Test the connection
        try:
            self._test_connection()
        except Exception as e:
            logger.error(f"Failed to connect to OpenRouter API: {e}")
            raise
    
    def _test_connection(self):
        """Test API connection with a simple request"""
        try:
            response = self.client.post(
                self.api_url,
                json={
                    "model": self.model_name,
                    "messages": [
                        {"role": "user", "content": "Say 'test successful' and nothing else"}
                    ]
                }
            )
            response.raise_for_status()
            logger.info("✓ API connection test successful")
        except Exception as e:
            raise ValueError(f"Failed to connect to OpenRouter API: {e}")
    
    def _call_api(self, messages: List[Dict], retry_count: int = 3) -> str:
        """
        Make API call to OpenRouter
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            retry_count: Number of retries on failure
            
        Returns:
            Response text from the model
        """
        for attempt in range(retry_count):
            try:
                response = self.client.post(
                    self.api_url,
                    json={
                        "model": self.model_name,
                        "messages": messages
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                return content.strip()
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Rate limit - wait longer
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API error (attempt {attempt + 1}/{retry_count}): {e}")
                    if attempt == retry_count - 1:
                        raise
                    time.sleep(1)
            except Exception as e:
                logger.error(f"API call error (attempt {attempt + 1}/{retry_count}): {e}")
                if attempt == retry_count - 1:
                    raise
                time.sleep(1)
        
        raise Exception("Max retries exceeded")
    
    def enrich_bio(self, bio: str, retry_count: int = 3) -> EnrichmentResult:
        """
        Enrich a single bio with AI analysis
        
        Args:
            bio: Member bio/comment text
            retry_count: Number of retries on failure
        """
        user_prompt = self.prompt_manager.get_enrichment_prompt(bio)
        system_context = self.prompt_manager.get_system_context()
        
        messages = [
            {"role": "system", "content": system_context},
            {"role": "user", "content": user_prompt}
        ]
        
        for attempt in range(retry_count):
            try:
                raw_text = self._call_api(messages, retry_count=1)
                
                # Extract JSON from response (handle markdown code blocks)
                json_text = raw_text
                if '```json' in raw_text:
                    json_text = raw_text.split('```json')[1].split('```')[0].strip()
                elif '```' in raw_text:
                    json_text = raw_text.split('```')[1].split('```')[0].strip()
                
                # Parse JSON
                data = json.loads(json_text)
                
                # Validate and normalize
                result = EnrichmentResult(
                    skills=data.get('skills', []),
                    persona=data.get('persona', 'Unknown'),
                    confidence_score=float(data.get('confidence_score', 0)) / 100.0,  # Normalize to 0-1
                    reasoning=data.get('reasoning', ''),
                    raw_response=raw_text
                )
                
                # Validate confidence score
                if not 0 <= result.confidence_score <= 1:
                    result.confidence_score = max(0, min(1, result.confidence_score))
                
                logger.debug(f"Enriched bio: {bio[:50]}... -> Persona: {result.persona}, Confidence: {result.confidence_score:.2f}")
                
                return result
                
            except json.JSONDecodeError as e:
                logger.warning(f"Attempt {attempt + 1}/{retry_count} - JSON parse error: {e}")
                if attempt == retry_count - 1:
                    # Return low-confidence fallback
                    return EnrichmentResult(
                        skills=[],
                        persona="Unknown",
                        confidence_score=0.0,
                        reasoning="Failed to parse AI response",
                        raw_response=raw_text if 'raw_text' in locals() else ""
                    )
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{retry_count} - Enrichment error: {e}")
                if attempt == retry_count - 1:
                    return EnrichmentResult(
                        skills=[],
                        persona="Error",
                        confidence_score=0.0,
                        reasoning=f"Error: {str(e)}",
                        raw_response=""
                    )
                time.sleep(1)
        
        # Should never reach here
        return EnrichmentResult(
            skills=[],
            persona="Unknown",
            confidence_score=0.0,
            reasoning="Max retries exceeded",
            raw_response=""
        )
    
    def enrich_batch(self, bios: List[Dict], delay: float = 1.0) -> List[Dict]:
        """
        Enrich multiple bios with rate limiting
        
        Args:
            bios: List of dicts with 'member_name' and 'bio_or_comment'
            delay: Delay between API calls (seconds)
        """
        enriched = []
        
        for i, record in enumerate(bios):
            logger.info(f"Enriching {i+1}/{len(bios)}: {record['member_name']}")
            
            result = self.enrich_bio(record['bio_or_comment'])
            
            enriched.append({
                'member_name': record['member_name'],
                'skills': result.skills,
                'persona': result.persona,
                'confidence_score': result.confidence_score,
                'reasoning': result.reasoning,
                'raw_response': result.raw_response
            })
            
            # Rate limiting
            if i < len(bios) - 1:
                time.sleep(delay)
        
        return enriched
    
    def __del__(self):
        """Cleanup HTTP client"""
        if hasattr(self, 'client'):
            self.client.close()


if __name__ == "__main__":
    # Test enrichment
    test_bio = "Working with python and derivatives trading for 5+ years. Happy to mentor juniors."
    
    # You'll need to set your API key
    enricher = AIEnricher()
    result = enricher.enrich_bio(test_bio)
    
    print(f"Skills: {result.skills}")
    print(f"Persona: {result.persona}")
    print(f"Confidence: {result.confidence_score:.2f}")
    print(f"Reasoning: {result.reasoning}")