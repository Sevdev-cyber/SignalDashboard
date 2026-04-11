"""DeepSeek API client for LLM-based trading decisions.

Handles HTTP communication, retries, timeouts, and JSON extraction from
LLM responses. Falls back gracefully if API is unavailable.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)

_DEFAULT_URL = "https://api.deepseek.com/v1/chat/completions"
_DEFAULT_MODEL = "deepseek-chat"
_DEFAULT_TIMEOUT = 30


@dataclass(slots=True)
class LLMResponse:
    """Parsed LLM response."""

    raw_text: str = ""
    parsed_json: dict | None = None
    model: str = ""
    elapsed_sec: float = 0.0
    success: bool = False
    error: str = ""


class DeepSeekClient:
    """HTTP client for DeepSeek chat completions API."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = _DEFAULT_MODEL,
        timeout: int | None = None,
        url: str = _DEFAULT_URL,
    ) -> None:
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY", "")
        self.model = model
        self.timeout = timeout or int(os.environ.get("DEEPSEEK_TIMEOUT_SEC", _DEFAULT_TIMEOUT))
        self.url = url

        if not self.api_key:
            log.warning("No DEEPSEEK_API_KEY — LLM calls will fail")

    def call(self, system_prompt: str, user_prompt: str, temperature: float = 0.1) -> LLMResponse:
        """Send a chat completion request and parse the JSON response."""
        if not self.api_key:
            return LLMResponse(error="no_api_key", success=False)

        start = time.monotonic()

        try:
            import urllib.request
            import ssl

            payload = json.dumps({
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
                "response_format": {"type": "json_object"},
            }).encode("utf-8")

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            # SSL context
            try:
                import certifi
                ctx = ssl.create_default_context(cafile=certifi.where())
            except ImportError:
                ctx = ssl.create_default_context()

            req = urllib.request.Request(self.url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=self.timeout, context=ctx) as resp:
                body = json.loads(resp.read().decode("utf-8"))

            elapsed = time.monotonic() - start
            raw_text = body.get("choices", [{}])[0].get("message", {}).get("content", "")
            model = body.get("model", self.model)

            # Parse JSON from response
            parsed = self._extract_json(raw_text)

            return LLMResponse(
                raw_text=raw_text,
                parsed_json=parsed,
                model=model,
                elapsed_sec=round(elapsed, 2),
                success=parsed is not None,
                error="" if parsed else "json_parse_failed",
            )

        except Exception as e:
            elapsed = time.monotonic() - start
            log.error("DeepSeek API error (%.1fs): %s", elapsed, e)
            return LLMResponse(
                elapsed_sec=round(elapsed, 2),
                success=False,
                error=str(e),
            )

    def _extract_json(self, text: str) -> dict | None:
        """Extract JSON from LLM response text."""
        text = text.strip()

        # Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try extracting from markdown code block
        if "```" in text:
            for block in text.split("```"):
                block = block.strip()
                if block.startswith("json"):
                    block = block[4:].strip()
                try:
                    return json.loads(block)
                except json.JSONDecodeError:
                    continue

        # Try finding JSON object
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass

        return None
