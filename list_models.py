import os
import httpx
import json

# Get API key
api_key = os.getenv('OPENROUTER_API_KEY')
if not api_key:
    print("Error: OPENROUTER_API_KEY not set")
    exit(1)

print("Fetching available models from OpenRouter...\n")

try:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    with httpx.Client(timeout=30.0, headers=headers) as client:
        response = client.get("https://openrouter.ai/api/v1/models")
        response.raise_for_status()
        data = response.json()
        
        models = data.get("data", [])
        
        print("Available models:")
        print("-" * 80)
        
        # Group by provider
        providers = {}
        for model in models:
            provider = model.get("id", "").split("/")[0] if "/" in model.get("id", "") else "unknown"
            if provider not in providers:
                providers[provider] = []
            providers[provider].append(model)
        
        # Print models grouped by provider
        for provider in sorted(providers.keys()):
            print(f"\n{provider.upper()}:")
            for model in sorted(providers[provider], key=lambda x: x.get("id", "")):
                model_id = model.get("id", "")
                name = model.get("name", model_id)
                context_length = model.get("context_length", "N/A")
                pricing = model.get("pricing", {})
                
                print(f"  ✓ {model_id}")
                print(f"    Name: {name}")
                if context_length != "N/A":
                    print(f"    Context: {context_length:,} tokens")
                if pricing:
                    prompt_price = pricing.get("prompt", "N/A")
                    completion_price = pricing.get("completion", "N/A")
                    print(f"    Pricing: ${prompt_price}/1M prompt, ${completion_price}/1M completion")
                print()
        
        print("-" * 80)
        print("\nRecommended models to use:")
        print("  - openai/gpt-4o-mini (cost-effective, fast)")
        print("  - openai/gpt-4o (better quality)")
        print("  - anthropic/claude-3-haiku (fast, good quality)")
        print("  - google/gemini-2.0-flash-exp (latest Gemini)")
        print("\nNote: You can use any model ID from the list above in your code.")
        
except httpx.HTTPStatusError as e:
    print(f"Error: {e.response.status_code} - {e.response.text}")
    exit(1)
except Exception as e:
    print(f"Error: {e}")
    exit(1)