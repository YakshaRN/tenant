import json
import re


def extract_json(text: str):
    """Extract and parse JSON from LLM output, handling common issues."""
    text = text.strip()

    # Remove markdown code fences
    text = re.sub(r'```[a-zA-Z]*\n?', '', text)
    text = text.replace('```', '')
    text = text.strip()

    start = text.find('{')
    end = text.rfind('}')

    if start == -1 or end == -1:
        raise ValueError("No JSON object found in response")

    candidate = text[start:end+1]

    # Attempt 1: direct parse
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass

    # Attempt 2: fix trailing commas
    cleaned = re.sub(r',\s*}', '}', candidate)
    cleaned = re.sub(r',\s*]', ']', cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Attempt 3: remove JS-style comments
    cleaned2 = re.sub(r'//[^\n]*', '', cleaned)
    cleaned2 = re.sub(r'/\*.*?\*/', '', cleaned2, flags=re.DOTALL)
    try:
        return json.loads(cleaned2)
    except json.JSONDecodeError:
        pass

    # Attempt 4: strip control characters
    cleaned3 = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', cleaned2)
    try:
        return json.loads(cleaned3)
    except json.JSONDecodeError as e:
        raise ValueError(f"Could not parse JSON: {e}\nFirst 300 chars: {candidate[:300]}")
