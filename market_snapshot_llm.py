"""Optional chat-completions wrapper for market snapshot prompts.

This is intentionally vendor-agnostic. It talks to any OpenAI-compatible
chat-completions endpoint and only returns parsed JSON when the model emits it.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from env_bootstrap import load_project_env

load_project_env()

log = logging.getLogger(__name__)


@dataclass(slots=True)
class SnapshotLLMResponse:
    raw_text: str = ""
    parsed_json: dict[str, Any] | None = None
    model: str = ""
    elapsed_sec: float = 0.0
    success: bool = False
    error: str = ""


class MarketSnapshotLLMClient:
    """Simple OpenAI-compatible chat client for structured snapshot prompts."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        model: str | None = None,
        base_url: str | None = None,
        timeout_sec: int | None = None,
    ) -> None:
        self.provider = os.environ.get("SNAPSHOT_LLM_PROVIDER", "openai").strip().lower()
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY") or os.environ.get("DEEPSEEK_API_KEY") or ""
        self.model = model or os.environ.get(
            "SNAPSHOT_LLM_MODEL",
            "gpt-4.1-mini" if self.provider == "openai" else "deepseek-chat",
        )
        self.base_url = base_url or os.environ.get(
            "SNAPSHOT_LLM_BASE_URL",
            "https://api.openai.com/v1/chat/completions" if self.provider == "openai" else "https://api.deepseek.com/v1/chat/completions",
        )
        self.timeout_sec = timeout_sec or int(os.environ.get("SNAPSHOT_LLM_TIMEOUT_SEC", "30"))

    def call(self, system_prompt: str, user_prompt: str, temperature: float = 0.1) -> SnapshotLLMResponse:
        if not self.api_key:
            return SnapshotLLMResponse(error="no_api_key", success=False)

        start = time.monotonic()
        try:
            import ssl
            import urllib.request

            payload = json.dumps(
                {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": temperature,
                    "response_format": {"type": "json_object"},
                }
            ).encode("utf-8")

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            try:
                import certifi
                ctx = ssl.create_default_context(cafile=certifi.where())
            except Exception:
                ctx = ssl.create_default_context()

            req = urllib.request.Request(self.base_url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=self.timeout_sec, context=ctx) as resp:
                body = json.loads(resp.read().decode("utf-8"))

            raw_text = body.get("choices", [{}])[0].get("message", {}).get("content", "")
            parsed = self._extract_json(raw_text)
            elapsed = time.monotonic() - start

            return SnapshotLLMResponse(
                raw_text=raw_text,
                parsed_json=parsed,
                model=body.get("model", self.model),
                elapsed_sec=round(elapsed, 2),
                success=parsed is not None,
                error="" if parsed else "json_parse_failed",
            )
        except Exception as e:
            elapsed = time.monotonic() - start
            log.error("Snapshot LLM error (%.1fs): %s", elapsed, e)
            return SnapshotLLMResponse(elapsed_sec=round(elapsed, 2), success=False, error=str(e))

    def _extract_json(self, text: str) -> dict[str, Any] | None:
        text = text.strip()
        try:
            value = json.loads(text)
            return value if isinstance(value, dict) else None
        except json.JSONDecodeError:
            pass

        if "```" in text:
            for block in text.split("```"):
                block = block.strip()
                if block.startswith("json"):
                    block = block[4:].strip()
                try:
                    value = json.loads(block)
                    if isinstance(value, dict):
                        return value
                except json.JSONDecodeError:
                    continue

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                value = json.loads(text[start : end + 1])
                if isinstance(value, dict):
                    return value
            except json.JSONDecodeError:
                pass
        return None
