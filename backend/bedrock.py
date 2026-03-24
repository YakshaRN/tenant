import json
import os
import logging

import boto3
from botocore.config import Config

logger = logging.getLogger("bedrock")

MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "amazon.nova-micro-v1:0")
MAX_TOKENS = int(os.getenv("BEDROCK_MAX_TOKENS", "4000"))

_client = None


def _get_client():
    global _client
    if _client is None:
        region = os.getenv("AWS_REGION", "us-east-1")
        cfg = Config(
            region_name=region,
            read_timeout=300,
            retries={"max_attempts": 2},
        )
        _client = boto3.client("bedrock-runtime", config=cfg)
        logger.info(f"Bedrock client created: region={region}, model={MODEL_ID}")
    return _client


def call_llm(prompt, system_prompt=None, max_tokens=None):
    client = _get_client()
    tokens = max_tokens or MAX_TOKENS
    logger.info(f"Calling Bedrock: model={MODEL_ID}, max_tokens={tokens}, prompt_len={len(prompt)}")

    # Anthropic models use invoke_model with anthropic payload.
    if "anthropic" in MODEL_ID:
        messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": tokens,
            "temperature": 0,
            "top_p": 0.1,
            "messages": messages,
        }
        if system_prompt:
            body["system"] = [{"type": "text", "text": system_prompt}]

        response = client.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body),
        )
        result = json.loads(response["body"].read())
        text = result["content"][0]["text"]
        logger.info(f"Bedrock response: {len(text)} chars, stop_reason={result.get('stop_reason', '?')}")
        return text

    # Default path for modern Bedrock models (e.g. Nova) via Converse API.
    messages = [{"role": "user", "content": [{"text": prompt}]}]
    kwargs = {
        "modelId": MODEL_ID,
        "messages": messages,
        "inferenceConfig": {
            "maxTokens": tokens,
            "temperature": 0,
            "topP": 0.1,
        },
    }
    if system_prompt:
        kwargs["system"] = [{"text": system_prompt}]

    response = client.converse(**kwargs)
    parts = response.get("output", {}).get("message", {}).get("content", [])
    text = "".join(part.get("text", "") for part in parts if isinstance(part, dict))
    logger.info(f"Bedrock converse response: {len(text)} chars")
    return text
