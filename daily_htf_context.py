"""Daily higher-timeframe context cache with optional LLM enrichment.

The daily context is intentionally cached once per date. The deterministic
HTF audit remains the source of truth; the LLM layer only turns that audit
into a compact daily/weekly/monthly playbook when an API key is available.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from env_bootstrap import load_project_env
from market_snapshot_llm import MarketSnapshotLLMClient

load_project_env()

log = logging.getLogger("signal_dash")


_PRICE_RE = re.compile(r"(?<![\d.])(\d{4,6}(?:\.\d+)?)")


def _extract_price_tokens(text: str) -> list[float]:
    if not text:
        return []
    out: list[float] = []
    for match in _PRICE_RE.finditer(text):
        try:
            out.append(float(match.group(1)))
        except Exception:
            continue
    return out


def _daily_refs(payload: dict[str, Any]) -> tuple[list[float], float, float]:
    current_price = float(payload.get("current_price") or 0.0)
    refs = [current_price]
    htf = payload.get("htf_audit") if isinstance(payload.get("htf_audit"), dict) else {}
    for item in list(htf.get("levels") or []):
        if isinstance(item, dict):
            try:
                refs.append(float(item.get("price") or 0.0))
            except Exception:
                continue
    for item in list(htf.get("active_fvgs") or []):
        if isinstance(item, dict):
            for key in ("low", "high", "top", "bottom"):
                try:
                    refs.append(float(item.get(key) or 0.0))
                except Exception:
                    continue
    for guide in (payload.get("timeframes") or {}).values():
        if isinstance(guide, dict):
            for key in ("trigger_level", "invalidation_level", "range_high", "range_low"):
                try:
                    refs.append(float(guide.get(key) or 0.0))
                except Exception:
                    continue
    refs = [round(x, 2) for x in refs if x > 0]
    max_distance = max(120.0, current_price * 0.006) if current_price else 120.0
    near_band = 12.0
    return refs, max_distance, near_band


def _text_uses_valid_prices(text: str, refs: list[float], max_distance: float, near_band: float, current_price: float) -> bool:
    for value in _extract_price_tokens(text):
        if current_price and abs(value - current_price) <= max_distance:
            continue
        if refs and min(abs(value - ref) for ref in refs) <= near_band:
            continue
        return False
    return True


def _load_external_htf() -> dict[str, Any]:
    raw_path = os.environ.get("SIGNAL_EXTERNAL_HTF_PATH", "").strip()
    if not raw_path:
        return {}
    path = Path(raw_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


class DailyHTFContextService:
    """Stores one daily HTF context payload and refreshes it on demand."""

    def __init__(self, cache_path: Path | None = None) -> None:
        runtime_dir = Path(__file__).resolve().parent / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = cache_path or runtime_dir / "daily_htf_context.json"
        self.enabled = os.environ.get("SIGNAL_DAILY_CONTEXT_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
        self.use_llm = os.environ.get("SIGNAL_DAILY_CONTEXT_USE_LLM", "1").strip().lower() not in {"0", "false", "no"}
        self.refresh_hours = max(1, int(os.environ.get("SIGNAL_CONTEXT_REFRESH_HOURS", "1") or "1"))
        self._latest: dict[str, Any] | None = None

    def get_context(self, now: datetime | None = None) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        now = now or datetime.utcnow()
        target_bucket = self._bucket_key(now)
        if self._latest and self._latest.get("bucket") == target_bucket:
            return dict(self._latest)
        if not self.cache_path.exists():
            return None
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.debug("daily HTF cache read failed: %s", e)
            return None
        if not isinstance(payload, dict) or payload.get("bucket") != target_bucket:
            return None
        self._latest = payload
        return dict(payload)

    def maybe_refresh(
        self,
        *,
        bars_df: pd.DataFrame,
        trader_guide: dict[str, Any],
        current_price: float,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        now = now or datetime.utcnow()
        cached = self.get_context(now)
        if cached:
            return cached

        prompt_payload = self._build_prompt_payload(
            bars_df=bars_df,
            trader_guide=trader_guide,
            current_price=current_price,
            now=now,
        )
        context = self._build_fallback_context(prompt_payload)
        if self.use_llm:
            llm_payload = self._call_llm(prompt_payload)
            if llm_payload:
                context.update(llm_payload)
                context["source"] = "llm"
            else:
                context["source"] = "deterministic"
        else:
            context["source"] = "deterministic"

        context["date"] = now.date().isoformat()
        context["bucket"] = self._bucket_key(now)
        context["generated_at"] = now.isoformat()
        self._latest = context
        try:
            self.cache_path.write_text(json.dumps(context, ensure_ascii=True, indent=2), encoding="utf-8")
        except Exception as e:
            log.debug("daily HTF cache write failed: %s", e)
        return dict(context)

    def _bucket_key(self, now: datetime) -> str:
        bucket_hour = now.hour - (now.hour % self.refresh_hours)
        return f"{now.date().isoformat()}T{bucket_hour:02d}"

    def _build_prompt_payload(
        self,
        *,
        bars_df: pd.DataFrame,
        trader_guide: dict[str, Any],
        current_price: float,
        now: datetime,
    ) -> dict[str, Any]:
        htf = dict(trader_guide.get("htf_audit") or {})
        external_htf = _load_external_htf()
        guides = {}
        for key in ("tf_1h", "tf_4h", "tf_1d", "tf_1w", "tf_1mo"):
            guide = trader_guide.get(key)
            if isinstance(guide, dict):
                guides[key] = {
                    "timeframe": guide.get("timeframe"),
                    "bias": guide.get("bias"),
                    "strength": guide.get("strength"),
                    "history_bars": guide.get("history_bars"),
                    "history_ok": guide.get("history_ok"),
                    "trigger_level": guide.get("trigger_level"),
                    "invalidation_level": guide.get("invalidation_level"),
                    "range_high": guide.get("range_high"),
                    "range_low": guide.get("range_low"),
                    "continuation_note": guide.get("continuation_note"),
                    "reversal_note": guide.get("reversal_note"),
                    "active_fvgs": list(guide.get("active_fvgs") or []),
                }
        if isinstance(external_htf.get("timeframes"), dict):
            for key, guide in external_htf.get("timeframes", {}).items():
                if key not in {"tf_1h", "tf_4h", "tf_1d", "tf_1w", "tf_1mo"}:
                    continue
                if not isinstance(guide, dict):
                    continue
                merged = dict(guides.get(key) or {})
                merged.update(
                    {
                        "timeframe": guide.get("timeframe", merged.get("timeframe")),
                        "bias": guide.get("bias", merged.get("bias")),
                        "strength": guide.get("strength", merged.get("strength")),
                        "history_bars": guide.get("history_bars", merged.get("history_bars")),
                        "history_ok": guide.get("history_ok", merged.get("history_ok", True)),
                        "trigger_level": guide.get("trigger_level", merged.get("trigger_level")),
                        "invalidation_level": guide.get("invalidation_level", merged.get("invalidation_level")),
                        "range_high": guide.get("range_high", merged.get("range_high")),
                        "range_low": guide.get("range_low", merged.get("range_low")),
                        "continuation_note": guide.get("continuation_note", merged.get("continuation_note")),
                        "reversal_note": guide.get("reversal_note", merged.get("reversal_note")),
                        "active_fvgs": list(guide.get("active_fvgs") or merged.get("active_fvgs") or []),
                    }
                )
                guides[key] = merged

        external_levels = [x for x in list(external_htf.get("levels") or []) if isinstance(x, dict)]
        external_fvgs = [x for x in list(external_htf.get("active_fvgs") or []) if isinstance(x, dict)]
        merged_levels = list(htf.get("levels") or []) + external_levels
        merged_fvgs = list(htf.get("active_fvgs") or []) + external_fvgs
        history_note = str(htf.get("history_note") or "")
        if external_htf.get("history_note"):
            history_note = (history_note + " | " if history_note else "") + str(external_htf.get("history_note"))
        weekly_context = external_htf.get("weekly_context", htf.get("weekly_context"))
        monthly_context = external_htf.get("monthly_context", htf.get("monthly_context"))
        summary = external_htf.get("summary", htf.get("summary"))
        preferred_playbooks = list(external_htf.get("preferred_playbooks") or htf.get("preferred_playbooks") or [])
        avoid_conditions = list(external_htf.get("avoid_conditions") or htf.get("avoid_conditions") or [])
        external_source = str(external_htf.get("source") or "").strip()

        return {
            "date": now.date().isoformat(),
            "current_price": round(float(current_price or 0.0), 2),
            "bars_loaded": int(len(bars_df)),
            "overall_bias": trader_guide.get("overall_bias"),
            "guide_summary": trader_guide.get("summary"),
            "style_rules": {
                "language": "pl",
                "mode": "quick",
                "priority": [
                    "najpierw HTF i spojność danych",
                    "potem 1H/4H/1D alignment",
                    "potem kluczowe poziomy i FVG",
                    "bez wymyślania nowych poziomów cenowych",
                    "gdy HTF jest mixed, opisz to wprost i nie udawaj pewności",
                ],
            },
            "htf_audit": {
                "pre_signal_bias": htf.get("pre_signal_bias"),
                "pre_signal_confidence": htf.get("pre_signal_confidence"),
                "summary": summary,
                "weekly_context": weekly_context,
                "monthly_context": monthly_context,
                "preferred_playbooks": preferred_playbooks[:6],
                "avoid_conditions": avoid_conditions[:6],
                "history_note": history_note,
                "levels": merged_levels[:12],
                "active_fvgs": merged_fvgs[:8],
            },
            "timeframes": guides,
            "external_htf_source": external_source,
        }

    def _build_fallback_context(self, payload: dict[str, Any]) -> dict[str, Any]:
        htf = payload.get("htf_audit") or {}
        bias = str(htf.get("pre_signal_bias") or "neutral")
        levels = list(htf.get("levels") or [])
        active_fvgs = list(htf.get("active_fvgs") or [])
        level_lines = [
            f"{item.get('label')} {float(item.get('price') or 0.0):.2f}"
            for item in levels[:5]
        ]
        return {
            "daily_bias": bias,
            "summary": str(htf.get("summary") or payload.get("guide_summary") or "No daily HTF summary."),
            "weekly_context": str(htf.get("weekly_context") or "Weekly context unavailable."),
            "monthly_context": str(htf.get("monthly_context") or "Monthly context unavailable."),
            "playbook_priority": list(htf.get("preferred_playbooks") or []),
            "avoid_conditions": list(htf.get("avoid_conditions") or []),
            "levels_to_watch": level_lines,
            "active_fvgs": active_fvgs,
            "history_note": str(htf.get("history_note") or ""),
            "llm_ok": False,
        }

    def _call_llm(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        client = MarketSnapshotLLMClient()
        response = client.call(
            (
                "Jesteś warstwą HTF dla bota MNQ. "
                "Nie analizujesz rynku od zera; porządkujesz deterministyczny audit do krótkiego playbooka. "
                "Zwróć tylko JSON. Pisz po polsku, technicznie i zwięźle. "
                "Używaj wyłącznie poziomów cenowych obecnych w payloadzie. "
                "Nie wymyślaj nowych poziomów, nowych struktur ani nowych timeframe'ów. "
                "Jeżeli 1W lub 1M są niepełne, napisz to wprost. "
                "Jeżeli HTF jest mieszany, zachowaj neutralność zamiast sztucznej pewności."
            ),
            (
                "Zwróć JSON z kluczami: "
                "daily_bias, summary, weekly_context, monthly_context, "
                "playbook_priority, avoid_conditions, levels_to_watch. "
                "summary ma mieć 1-3 krótkie zdania, egzekucyjne, bez lania wody. "
                "levels_to_watch mają być krótkimi liniami tekstu opartymi tylko o poziomy z payloadu.\n\n"
                f"{json.dumps(payload, ensure_ascii=True, indent=2, default=str)}"
            ),
            temperature=0.0,
        )
        if not response.success or not isinstance(response.parsed_json, dict):
            if response.error:
                log.info("Daily HTF LLM skipped: %s", response.error)
            return None

        parsed = response.parsed_json
        refs, max_distance, near_band = _daily_refs(payload)
        current_price = float(payload.get("current_price") or 0.0)
        texts_to_validate = [
            str(parsed.get("summary") or ""),
            str(parsed.get("weekly_context") or ""),
            str(parsed.get("monthly_context") or ""),
            *[str(x) for x in list(parsed.get("levels_to_watch") or [])[:8]],
        ]
        if not all(_text_uses_valid_prices(text, refs, max_distance, near_band, current_price) for text in texts_to_validate):
            log.info("Daily HTF LLM rejected: invalid price references in summary.")
            return None
        return {
            "daily_bias": str(parsed.get("daily_bias") or payload.get("htf_audit", {}).get("pre_signal_bias") or "neutral"),
            "summary": str(parsed.get("summary") or payload.get("htf_audit", {}).get("summary") or ""),
            "weekly_context": str(parsed.get("weekly_context") or payload.get("htf_audit", {}).get("weekly_context") or ""),
            "monthly_context": str(parsed.get("monthly_context") or payload.get("htf_audit", {}).get("monthly_context") or ""),
            "playbook_priority": [str(x) for x in list(parsed.get("playbook_priority") or [])[:6]],
            "avoid_conditions": [str(x) for x in list(parsed.get("avoid_conditions") or [])[:6]],
            "levels_to_watch": [str(x) for x in list(parsed.get("levels_to_watch") or [])[:8]],
            "active_fvgs": list(payload.get("htf_audit", {}).get("active_fvgs") or []),
            "history_note": str(payload.get("htf_audit", {}).get("history_note") or ""),
            "llm_ok": True,
            "llm_model": response.model,
        }
