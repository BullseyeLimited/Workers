import json


def _strip_fences(text: str) -> str:
    """
    Remove ``` and ```json fences if the model used them.
    """
    s = (text or "").strip()
    if not s:
        return s

    if s.startswith("```"):
        # Drop the first line (``` or ```json)
        newline = s.find("\n")
        if newline != -1:
            s = s[newline + 1 :]
        s = s.strip()
        # Drop trailing ```
        if s.endswith("```"):
            s = s[:-3]
        s = s.strip()

    return s


def extract_json_object(text: str) -> dict:
    """
    Robustly pull a JSON object out of an LLM's output.

    - Strips code fences
    - Grabs the substring between the first '{' and the last '}'
    - json.loads() that substring
    """
    s = _strip_fences(text)
    if not s:
        raise ValueError("model returned empty string")

    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or start >= end:
        raise ValueError("no JSON object found in model output")

    json_str = s[start : end + 1]
    return json.loads(json_str)


def safe_parse_model_json(text: str):
    """
    Returns (analysis_dict, error_message).

    - On success: (dict, None)
    - On failure: (None, 'ErrorType: message')
    """
    try:
        data = extract_json_object(text)
        return data, None
    except Exception as e:  # noqa: BLE001
        return None, f"{e.__class__.__name__}: {e}"
