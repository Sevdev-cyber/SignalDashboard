"""Profile configuration with inheritance support.

Replaces V1's 11 near-identical JSON files with base + overrides.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class TCPConfig:
    host: str = "127.0.0.1"
    port: int = 5556
    account_name: str = "Sim101"
    dry_run: bool = True


@dataclass(slots=True)
class Profile:
    """Unified runtime profile — replaces V1's TickStreamerProfile/PaperBridgeProfile split."""

    name: str = "default"
    variant: str = "same_tf_v5"
    session: str = "bars"
    micro_source: str = "same_tf"
    gate_mode: str = "off"
    api_mode: str = "fallback"  # fallback | macro | full

    # Runtime paths
    output_root: Path = field(default_factory=lambda: Path("runtime/default"))
    heartbeat_file: Path = field(default_factory=lambda: Path("runtime/default/state/heartbeat.json"))
    submitted_ids_file: Path = field(default_factory=lambda: Path("runtime/default/state/submitted_candidate_ids.json"))
    cursor_file: Path = field(default_factory=lambda: Path("runtime/default/state/last_submitted_ts.txt"))

    # Polling
    loop_poll_seconds: float = 5.0
    order_limit_per_cycle: int = 1
    stale_feed_seconds: float = 420.0
    require_flat_position: bool = True

    # LLM timing
    macro_review_minutes: int = 15
    micro_review_minutes: int = 5
    warmup_review_enabled: bool = False
    warmup_review_max_candidates: int = 3

    # TCP bridge
    tcp: TCPConfig = field(default_factory=TCPConfig)

    # Notes
    notes: str = ""

    @classmethod
    def load(cls, path: Path) -> Profile:
        """Load a profile with ``_extends`` inheritance support."""
        data = json.loads(path.read_text(encoding="utf-8"))

        # Handle inheritance
        extends = data.pop("_extends", None)
        if extends:
            base_path = path.parent / extends
            base_data = json.loads(base_path.read_text(encoding="utf-8"))
            base_data.pop("_extends", None)
            base_data = _deep_merge(base_data, data)
            data = base_data

        # Resolve env vars in strings
        data = _resolve_env_vars(data)

        # Build profile
        tcp_data = data.pop("tcp", {})
        tcp = TCPConfig(
            host=tcp_data.get("host", "127.0.0.1"),
            port=int(tcp_data.get("port", 5556)),
            account_name=tcp_data.get("account_name", "Sim101"),
            dry_run=bool(tcp_data.get("dry_run", True)),
        )

        output_root = Path(data.get("output_root", f"runtime/{data.get('name', 'default')}"))

        return cls(
            name=data.get("name", "default"),
            variant=data.get("variant", "same_tf_v5"),
            session=data.get("session", "bars"),
            micro_source=data.get("micro_source", "same_tf"),
            gate_mode=data.get("gate_mode", "off"),
            api_mode=data.get("api_mode", "fallback"),
            output_root=output_root,
            heartbeat_file=Path(data.get("heartbeat_file", str(output_root / "state" / "heartbeat.json"))),
            submitted_ids_file=Path(data.get("submitted_ids_file", str(output_root / "state" / "submitted_candidate_ids.json"))),
            cursor_file=Path(data.get("cursor_file", data.get("last_submitted_ts_file", str(output_root / "state" / "last_submitted_ts.txt")))),
            loop_poll_seconds=float(data.get("loop_poll_seconds", 5.0)),
            order_limit_per_cycle=int(data.get("order_limit_per_cycle", 1)),
            stale_feed_seconds=float(data.get("stale_feed_seconds", 420.0)),
            require_flat_position=bool(data.get("require_flat_position", True)),
            macro_review_minutes=int(data.get("macro_review_minutes", 15)),
            micro_review_minutes=int(data.get("micro_review_minutes", 5)),
            warmup_review_enabled=bool(data.get("warmup_review_enabled", False)),
            warmup_review_max_candidates=int(data.get("warmup_review_max_candidates", 3)),
            tcp=tcp,
            notes=data.get("notes", ""),
        )

    def apply_api_environment(self) -> None:
        """Set environment variables based on api_mode."""
        if self.api_mode == "fallback":
            os.environ["HYBRID_DISABLE_API"] = "1"
            os.environ["HYBRID_ENABLE_MACRO_API"] = "0"
            os.environ["HYBRID_ENABLE_MICRO_API"] = "0"
            os.environ["HYBRID_ENABLE_EVENT_API"] = "0"
        elif self.api_mode == "macro":
            os.environ["HYBRID_DISABLE_API"] = "0"
            os.environ["HYBRID_ENABLE_MACRO_API"] = "1"
            os.environ["HYBRID_ENABLE_MICRO_API"] = "0"
            os.environ["HYBRID_ENABLE_EVENT_API"] = "0"
        elif self.api_mode == "full":
            os.environ["HYBRID_DISABLE_API"] = "0"
            os.environ["HYBRID_ENABLE_MACRO_API"] = "1"
            os.environ["HYBRID_ENABLE_MICRO_API"] = "1"
            os.environ["HYBRID_ENABLE_EVENT_API"] = "1"


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _resolve_env_vars(data: dict) -> dict:
    """Replace ${VAR_NAME} placeholders with environment variable values."""
    result = {}
    for key, value in data.items():
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_key = value[2:-1]
            result[key] = os.environ.get(env_key, value)
        elif isinstance(value, dict):
            result[key] = _resolve_env_vars(value)
        else:
            result[key] = value
    return result
