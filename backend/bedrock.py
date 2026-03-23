import json
import os
import logging

import boto3
from botocore.config import Config

logger = logging.getLogger("bedrock")

MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-opus-4-6-v1")
MAX_TOKENS = int(os.getenv("BEDROCK_MAX_TOKENS", "16000"))

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

    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": tokens,
        "messages": messages,
    }

    if system_prompt:
        body["system"] = [{"type": "text", "text": system_prompt}]

    logger.info(f"Calling Bedrock: model={MODEL_ID}, max_tokens={tokens}, prompt_len={len(prompt)}")

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
