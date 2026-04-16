"""Intraday LLM context with 15m scheduler and event-gated refreshes.

This layer is advisory only:
- deterministic logic remains primary
- LLM is refreshed on a base schedule (default 15m)
- additional refreshes happen only on important state changes
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from env_bootstrap import load_project_env
from market_snapshot_llm import MarketSnapshotLLMClient

load_project_env()

log = logging.getLogger("signal_dash")


_PRICE_RE = re.compile(r"(?<![\d.])(\d{4,6}(?:\.\d+)?)")


def _norm(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return default
        return out
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _canonical_bias(value: Any) -> str:
    text = _norm(value, "neutral").lower()
    if text in {"long", "short", "neutral", "neutral_to_long", "neutral_to_short"}:
        return text
    if text.endswith("long"):
        return "neutral_to_long"
    if text.endswith("short"):
        return "neutral_to_short"
    if text in {"rotation", "mixed", "chop", "transition", "wait"}:
        return "neutral"
    return "neutral"


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


def _cluster_count(sig: dict[str, Any]) -> int:
    return _int(sig.get("confluence_count") or sig.get("same_dir_family_count") or sig.get("cluster"))


def _risk_points(sig: dict[str, Any]) -> float:
    entry = _num(sig.get("entry"))
    sl = _num(sig.get("sl"))
    if not entry or not sl:
        return 0.0
    return round(abs(entry - sl), 2)


def _target_r(sig: dict[str, Any]) -> float:
    entry = _num(sig.get("entry"))
    tp1 = _num(sig.get("tp1"))
    risk = max(_risk_points(sig), 0.25)
    if not entry or not tp1:
        return 0.0
    return round(abs(tp1 - entry) / risk, 2)


def _compact_signal(sig: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": _norm(sig.get("name") or sig.get("signal_kind"), "signal"),
        "direction": _norm(sig.get("direction"), "neutral"),
        "confidence_pct": _int(sig.get("confidence_pct")),
        "entry": round(_num(sig.get("entry")), 2),
        "sl": round(_num(sig.get("sl")), 2),
        "tp1": round(_num(sig.get("tp1")), 2),
        "cluster": _cluster_count(sig),
        "source_type": _norm(sig.get("source_type") or sig.get("lead_signal_kind")),
        "playbook_id": _norm(sig.get("_playbook_id") or sig.get("playbook_id")),
        "playbook_title": _norm(sig.get("_playbook_title") or sig.get("playbook_title")),
        "exec_score": round(_num(sig.get("exec_score") or sig.get("_exec_score")), 2),
        "target_r": _target_r(sig),
        "risk_pts": _risk_points(sig),
        "exec_notes": [_norm(x) for x in list(sig.get("exec_notes") or sig.get("_exec_notes") or [])[:6] if _norm(x)],
    }


def _compact_zone(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "label": _norm(item.get("label"), "zone"),
        "direction": _norm(item.get("direction"), "neutral"),
        "low": round(_num(item.get("low")), 2),
        "high": round(_num(item.get("high")), 2),
        "trigger": round(_num(item.get("trigger")), 2) if item.get("trigger") is not None else None,
        "why": _norm(item.get("why"), ""),
        "stage": _norm(item.get("stage"), ""),
    }


def _decision_ledger_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "direction": _norm(item.get("direction") or item.get("bias"), "neutral"),
        "status": _norm(item.get("status"), ""),
        "score": _int(item.get("score")),
        "reason": _norm(item.get("reason"), ""),
        "entry_mid": round(_num(item.get("entry_mid")), 2),
        "target_mid": round(_num(item.get("target_mid")), 2),
        "invalidation_level": round(_num(item.get("invalidation_level") or item.get("invalid_level")), 2),
        "scenario": _norm(item.get("scenario"), ""),
        "age_bars": _int(item.get("age_bars")),
    }


def _signal_stats(signals: list[dict[str, Any]], direction: str) -> dict[str, Any]:
    side = [sig for sig in signals if _norm(sig.get("direction")).lower() == direction]
    confs = [_int(sig.get("confidence_pct")) for sig in side]
    clusters = [_cluster_count(sig) for sig in side]
    return {
        "count": len(side),
        "best_confidence": max(confs) if confs else 0,
        "best_cluster": max(clusters) if clusters else 0,
        "avg_confidence": round(sum(confs) / len(confs), 1) if confs else 0.0,
    }


def _best_directional_signal(signals: list[dict[str, Any]], direction: str) -> dict[str, Any] | None:
    side = [sig for sig in signals if _norm(sig.get("direction")).lower() == direction]
    if not side:
        return None
    side.sort(
        key=lambda sig: (
            _num(sig.get("exec_score") or sig.get("_exec_score")),
            _int(sig.get("confidence_pct")),
            _cluster_count(sig),
        ),
        reverse=True,
    )
    return _compact_signal(side[0])


def _intraday_refs(snapshot: dict[str, Any]) -> tuple[list[float], float, float]:
    current_price = _num(snapshot.get("price"))
    atr = max(0.5, _num(snapshot.get("atr"), 4.0))
    refs = [
        current_price,
        _num(snapshot.get("vwap")),
        _num(snapshot.get("decision_trigger")),
        _num(snapshot.get("decision_invalidation")),
    ]
    entry_zone = snapshot.get("entry_zone") if isinstance(snapshot.get("entry_zone"), dict) else {}
    target_zone = snapshot.get("target_zone") if isinstance(snapshot.get("target_zone"), dict) else {}
    for zone in (entry_zone, target_zone):
        for key in ("low", "high"):
            refs.append(_num(zone.get(key)))
    for item in list(snapshot.get("top_levels") or []):
        if isinstance(item, dict):
            refs.append(_num(item.get("price")))
    for item in list(snapshot.get("top_signals") or []):
        if isinstance(item, dict):
            for key in ("entry", "sl", "tp1"):
                refs.append(_num(item.get(key)))
    for item in list(snapshot.get("execution_shortlist") or []):
        if isinstance(item, dict):
            for key in ("entry", "sl", "tp1"):
                refs.append(_num(item.get(key)))
    for item in list(snapshot.get("guide_zones") or []):
        if isinstance(item, dict):
            for key in ("low", "high", "trigger"):
                refs.append(_num(item.get(key)))
    for item in list(snapshot.get("decision_ledger_tail") or []):
        if isinstance(item, dict):
            for key in ("entry_mid", "target_mid", "invalidation_level"):
                refs.append(_num(item.get(key)))
    for key in ("best_long_entry", "best_short_entry"):
        refs.append(_num(snapshot.get(key)))
    refs = [round(x, 2) for x in refs if x > 0]
    max_distance = max(40.0, atr * 8.0)
    near_band = max(4.0, atr * 1.8)
    return refs, max_distance, near_band


def _text_prices_are_valid(text: str, refs: list[float], current_price: float, max_distance: float, near_band: float) -> bool:
    for value in _extract_price_tokens(text):
        if current_price and abs(value - current_price) <= max_distance:
            continue
        if refs and min(abs(value - ref) for ref in refs) <= near_band:
            continue
        return False
    return True


class IntradayLLMContextService:
    """Caches an intraday LLM read and refreshes only when needed."""

    def __init__(self, cache_path: Path | None = None) -> None:
        runtime_dir = Path(__file__).resolve().parent / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = cache_path or runtime_dir / "intraday_llm_context.json"
        self.enabled = os.environ.get("SIGNAL_INTRADAY_LLM_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
        self.use_llm = os.environ.get("SIGNAL_INTRADAY_LLM_USE_API", "1").strip().lower() not in {"0", "false", "no"}
        self.base_refresh_min = max(5, int(os.environ.get("SIGNAL_INTRADAY_LLM_BASE_MIN", "30") or "30"))
        self.event_cooldown_min = max(1, int(os.environ.get("SIGNAL_INTRADAY_LLM_EVENT_COOLDOWN_MIN", "15") or "15"))
        self.max_events_per_hour = max(1, int(os.environ.get("SIGNAL_INTRADAY_LLM_MAX_EVENTS_PER_HOUR", "1") or "1"))
        self.min_conf_delta = max(6, int(os.environ.get("SIGNAL_INTRADAY_LLM_MIN_CONF_DELTA", "12") or "12"))
        self.min_l2_conf = max(55, int(os.environ.get("SIGNAL_INTRADAY_LLM_MIN_L2_CONF", "72") or "72"))
        self._latest: dict[str, Any] | None = None

    def get_context(self, now: datetime | None = None) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        now = now or datetime.utcnow()
        if not self.cache_path.exists():
            return None
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.debug("intraday LLM cache read failed: %s", e)
            return None
        if not isinstance(payload, dict):
            return None
        return dict(payload)

    def maybe_refresh(self, payload: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        now = now or datetime.utcnow()
        cached = self.get_context(now)
        snapshot = self._extract_snapshot(payload, now=now)
        trigger = self._choose_trigger(snapshot, cached, now=now)
        if not trigger:
            return cached

        context = self._build_fallback_context(snapshot, trigger=trigger, cached=cached)
        if self.use_llm:
            llm_result = self._call_llm(snapshot, trigger=trigger)
            if llm_result:
                context.update(llm_result)
                context["source"] = "llm"
            else:
                context["source"] = "deterministic"
        else:
            context["source"] = "deterministic"

        hour_bucket = self._hour_bucket(now)
        prev_count = _int((cached or {}).get("event_calls_in_hour"))
        prev_hour = _norm((cached or {}).get("event_hour_bucket"))
        if hour_bucket != prev_hour:
            prev_count = 0
        if trigger["kind"] == "event":
            prev_count += 1

        context.update(
            {
                "generated_at": now.isoformat(),
                "scheduled_bucket": self._scheduled_bucket(now),
                "event_hour_bucket": hour_bucket,
                "event_calls_in_hour": prev_count,
                "trigger_type": trigger["type"],
                "trigger_label": trigger["label"],
                "snapshot_state": snapshot,
            }
        )
        try:
            self.cache_path.write_text(json.dumps(context, ensure_ascii=True, indent=2), encoding="utf-8")
        except Exception as e:
            log.debug("intraday LLM cache write failed: %s", e)
        return dict(context)

    def _scheduled_bucket(self, now: datetime) -> str:
        minute = now.minute - (now.minute % self.base_refresh_min)
        return now.replace(minute=minute, second=0, microsecond=0).isoformat()

    @staticmethod
    def _hour_bucket(now: datetime) -> str:
        return now.replace(minute=0, second=0, microsecond=0).isoformat()

    def _choose_trigger(self, snapshot: dict[str, Any], cached: dict[str, Any] | None, *, now: datetime) -> dict[str, str] | None:
        if not cached:
            return {"kind": "scheduled", "type": "bootstrap", "label": "Initial intraday context"}

        if _norm(cached.get("scheduled_bucket")) != self._scheduled_bucket(now):
            return {"kind": "scheduled", "type": "scheduled_15m", "label": "15m scheduled refresh"}

        prev = cached.get("snapshot_state") if isinstance(cached.get("snapshot_state"), dict) else {}
        event = self._detect_event(snapshot, prev)
        if not event:
            return None

        last_ts = _norm(cached.get("generated_at"))
        last_dt = None
        if last_ts:
            try:
                last_dt = datetime.fromisoformat(last_ts)
            except Exception:
                last_dt = None

        hour_bucket = self._hour_bucket(now)
        prev_hour = _norm(cached.get("event_hour_bucket"))
        event_calls = _int(cached.get("event_calls_in_hour"))
        if prev_hour != hour_bucket:
            event_calls = 0

        cooldown_ok = True
        if last_dt and event["kind"] != "hard_event":
            elapsed_min = (now - last_dt).total_seconds() / 60.0
            cooldown_ok = elapsed_min >= self.event_cooldown_min
        if not cooldown_ok:
            return None
        if event_calls >= self.max_events_per_hour and event["kind"] != "hard_event":
            return None
        return {"kind": "event", "type": event["type"], "label": event["label"]}

    def _detect_event(self, cur: dict[str, Any], prev: dict[str, Any]) -> dict[str, str] | None:
        cur_bias = _norm(cur.get("decision_bias"), "neutral")
        prev_bias = _norm(prev.get("decision_bias"), "neutral")
        cur_conf = _int(cur.get("decision_confidence"))
        prev_conf = _int(prev.get("decision_confidence"))
        cur_htf = _norm(cur.get("htf_bias"), "neutral")
        prev_htf = _norm(prev.get("htf_bias"), "neutral")
        cur_action = _norm(cur.get("decision_action"), "wait")
        prev_action = _norm(prev.get("decision_action"), "wait")
        cur_l2 = _norm(cur.get("l2_bias"), "neutral")
        prev_l2 = _norm(prev.get("l2_bias"), "neutral")
        cur_l2_conf = _int(cur.get("l2_confidence"))
        prev_primary = _norm(prev.get("primary_signal_id"))
        cur_primary = _norm(cur.get("primary_signal_id"))
        cur_clusters = _int(cur.get("strong_clusters"))
        prev_clusters = _int(prev.get("strong_clusters"))

        if cur_htf in {"long", "short"} and cur_htf != prev_htf:
            return {"kind": "hard_event", "type": "htf_bias_flip", "label": f"HTF flipped to {cur_htf}"}
        if cur_bias in {"long", "short"} and cur_bias != prev_bias and cur_conf >= 74:
            return {"kind": "event", "type": "decision_bias_flip", "label": f"Decision flipped to {cur_bias}"}
        if cur_action != prev_action and cur_conf >= 70:
            return {"kind": "event", "type": "action_shift", "label": f"Action changed to {cur_action}"}
        if abs(cur_conf - prev_conf) >= self.min_conf_delta and max(cur_conf, prev_conf) >= 74:
            return {"kind": "event", "type": "confidence_jump", "label": f"Confidence changed by {abs(cur_conf - prev_conf)}"}
        if cur_primary and cur_primary != prev_primary:
            return {"kind": "event", "type": "primary_signal_shift", "label": f"Primary signal changed to {cur_primary}"}
        if cur_l2 in {"long", "short"} and cur_l2 != prev_l2 and cur_l2_conf >= self.min_l2_conf:
            return {"kind": "event", "type": "micro_flow_flip", "label": f"L2 flipped to {cur_l2}"}
        if cur_clusters > prev_clusters and cur_clusters >= 1:
            return {"kind": "event", "type": "cluster_alignment", "label": f"Aligned clusters increased to {cur_clusters}"}
        return None

    def _extract_snapshot(self, payload: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        state = payload.get("state") if isinstance(payload.get("state"), dict) else {}
        guide = state.get("trader_guide") if isinstance(state.get("trader_guide"), dict) else {}
        htf = guide.get("htf_audit") if isinstance(guide.get("htf_audit"), dict) else {}
        daily = guide.get("daily_context") if isinstance(guide.get("daily_context"), dict) else {}
        l2 = guide.get("l2") if isinstance(guide.get("l2"), dict) else {}
        decision = payload.get("market_decision") if isinstance(payload.get("market_decision"), dict) else {}
        if not decision and isinstance(guide.get("market_decision"), dict):
            decision = dict(guide.get("market_decision") or {})
        execution = payload.get("execution") if isinstance(payload.get("execution"), dict) else {}
        if not execution and isinstance(guide.get("execution_view"), dict):
            execution = dict(guide.get("execution_view") or {})
        primary = execution.get("primary") if isinstance(execution.get("primary"), dict) else {}
        signals = payload.get("signals") if isinstance(payload.get("signals"), list) else []
        ghost_signals = payload.get("ghost_signals") if isinstance(payload.get("ghost_signals"), list) else []
        decision_ledger = payload.get("decision_ledger") if isinstance(payload.get("decision_ledger"), list) else []
        if not decision_ledger and isinstance(guide.get("decision_ledger"), list):
            decision_ledger = list(guide.get("decision_ledger") or [])
        zones = payload.get("zones") if isinstance(payload.get("zones"), dict) else {}
        continuation = guide.get("continuation") if isinstance(guide.get("continuation"), dict) else {}
        prediction = guide.get("prediction") if isinstance(guide.get("prediction"), dict) else {}
        best_long_zone = guide.get("best_long_zone") if isinstance(guide.get("best_long_zone"), dict) else {}
        best_short_zone = guide.get("best_short_zone") if isinstance(guide.get("best_short_zone"), dict) else {}
        execution_shortlist = list(execution.get("shortlist") or [])[:6]

        strong_clusters = 0
        top_signals = []
        for sig in signals[:6]:
            if not isinstance(sig, dict):
                continue
            cluster = _cluster_count(sig)
            if cluster >= 3:
                strong_clusters += 1
            top_signals.append(_compact_signal(sig))

        long_stats = _signal_stats(signals, "long")
        short_stats = _signal_stats(signals, "short")
        shortlist_long_stats = _signal_stats(execution_shortlist, "long")
        shortlist_short_stats = _signal_stats(execution_shortlist, "short")
        best_long = _best_directional_signal(execution_shortlist or signals, "long")
        best_short = _best_directional_signal(execution_shortlist or signals, "short")
        decision_bias = _norm(decision.get("bias"), "neutral").lower()
        aligned_side = decision_bias if decision_bias in {"long", "short"} else ""
        aligned_exec_score = 0.0
        opposite_exec_score = 0.0
        if aligned_side == "long":
            aligned_exec_score = _num((best_long or {}).get("exec_score"))
            opposite_exec_score = _num((best_short or {}).get("exec_score"))
        elif aligned_side == "short":
            aligned_exec_score = _num((best_short or {}).get("exec_score"))
            opposite_exec_score = _num((best_long or {}).get("exec_score"))
        signal_conflict = (
            aligned_side in {"long", "short"}
            and opposite_exec_score > 0
            and opposite_exec_score >= aligned_exec_score + 4.0
        )
        guide_zones = []
        for group in ("long", "short", "neutral"):
            for item in list(zones.get(group) or [])[:2]:
                if isinstance(item, dict):
                    guide_zones.append(_compact_zone(item))
        ledger_tail = [
            _decision_ledger_item(item)
            for item in list(decision_ledger)[-4:]
            if isinstance(item, dict)
        ]
        ghost_summary = []
        for sig in ghost_signals[:4]:
            if isinstance(sig, dict):
                ghost_summary.append(_compact_signal(sig))

        return {
            "timestamp": now.isoformat(),
            "price": round(_num(state.get("price")), 2),
            "atr": round(_num(state.get("atr")), 2),
            "vwap": round(_num(state.get("vwap")), 2),
            "vwap_dist_atr": round(_num(state.get("vwap_dist_atr")), 2),
            "ema_stack": _norm(state.get("ema_stack"), "NEUTRAL"),
            "regime": _norm(state.get("regime"), "unknown"),
            "delta_raw": round(_num(state.get("delta_raw")), 2),
            "cum_delta": round(_num(state.get("cum_delta")), 2),
            "cvd_trend": _norm(state.get("cvd_trend"), "FLAT"),
            "volume": round(_num(state.get("volume")), 2),
            "vol_ratio": round(_num(state.get("vol_ratio"), 1.0), 2),
            "decision_bias": _norm(decision.get("bias"), "neutral"),
            "decision_confidence": _int(decision.get("confidence")),
            "decision_action": _norm(decision.get("action"), "wait"),
            "decision_scenario": _norm(decision.get("scenario"), ""),
            "decision_summary": _norm(decision.get("summary"), ""),
            "decision_trigger": round(_num(decision.get("trigger_level")), 2),
            "decision_invalidation": round(_num(decision.get("invalidation_level")), 2),
            "entry_zone": decision.get("entry_zone") if isinstance(decision.get("entry_zone"), dict) else {},
            "target_zone": decision.get("target_zone") if isinstance(decision.get("target_zone"), dict) else {},
            "supporting_signals": [_norm(x) for x in list(decision.get("supporting_signals") or [])[:6] if _norm(x)],
            "prediction": {
                "side": _norm(prediction.get("side"), "neutral"),
                "trigger": round(_num(prediction.get("trigger")), 2),
                "watch_high": round(_num(prediction.get("watch_high")), 2),
                "watch_low": round(_num(prediction.get("watch_low")), 2),
                "message": _norm(prediction.get("message"), ""),
            },
            "htf_bias": _norm(htf.get("pre_signal_bias"), "neutral"),
            "htf_confidence": _int(htf.get("pre_signal_confidence")),
            "htf_summary": _norm(htf.get("summary"), ""),
            "daily_summary": _norm(daily.get("summary"), ""),
            "daily_bias": _norm(daily.get("daily_bias"), "neutral"),
            "daily_source": _norm(daily.get("source"), ""),
            "l2_bias": _norm(l2.get("micro_bias") or l2.get("display_bias"), "neutral"),
            "l2_confidence": _int(l2.get("display_confidence") or l2.get("confidence")),
            "l2_summary": _norm(l2.get("summary"), ""),
            "l2_flow_state": _norm(l2.get("flow_state"), ""),
            "l2_raw_bias": _norm(l2.get("raw_micro_bias"), ""),
            "l2_raw_confidence": _int(l2.get("raw_confidence")),
            "primary_signal_id": _norm(primary.get("name") or primary.get("signal_kind"), ""),
            "primary_signal_direction": _norm(primary.get("direction"), "neutral"),
            "primary_exec_score": round(_num(primary.get("exec_score")), 2),
            "primary_exec_notes": [_norm(x) for x in list(primary.get("exec_notes") or [])[:6] if _norm(x)],
            "shortlist_count": _int(execution.get("shortlist_count")),
            "raw_signal_count": _int(execution.get("raw_count")),
            "strong_clusters": strong_clusters,
            "signal_stats_long": long_stats,
            "signal_stats_short": short_stats,
            "shortlist_stats_long": shortlist_long_stats,
            "shortlist_stats_short": shortlist_short_stats,
            "best_long_signal": best_long,
            "best_short_signal": best_short,
            "best_long_entry": round(_num((best_long or {}).get("entry")), 2),
            "best_short_entry": round(_num((best_short or {}).get("entry")), 2),
            "aligned_exec_score": round(aligned_exec_score, 2),
            "opposite_exec_score": round(opposite_exec_score, 2),
            "signal_conflict": signal_conflict,
            "alignment_score": round(aligned_exec_score - opposite_exec_score, 2) if aligned_side else 0.0,
            "top_signals": top_signals,
            "top_levels": list((htf.get("levels") or [])[:6]),
            "top_fvgs": list((htf.get("active_fvgs") or [])[:4]),
            "execution_shortlist": [_compact_signal(sig) for sig in execution_shortlist if isinstance(sig, dict)],
            "ghost_signals": ghost_summary,
            "guide_zones": guide_zones[:6],
            "decision_ledger_tail": ledger_tail,
            "best_long_zone": _compact_zone(best_long_zone) if best_long_zone else {},
            "best_short_zone": _compact_zone(best_short_zone) if best_short_zone else {},
            "continuation": {
                "side": _norm(continuation.get("side"), "neutral"),
                "valid": bool(continuation.get("valid")),
                "note": _norm(continuation.get("note"), ""),
            },
            "style_rules": {
                "language": "pl",
                "mode": "quick_execution",
                "priority": [
                    "spojność danych i HTF",
                    "15m/5m struktura",
                    "L2 jako potwierdzenie, nie jako główny trigger",
                    "shortlist execution ma wyższy priorytet niż luźne top_signals",
                    "jeżeli sygnały są sprzeczne z decision_bias, nazwij konflikt wprost",
                    "bez wymyślania nowych poziomów cenowych",
                    "gdy rynek mixed/chop, preferuj wait nad fałszywą pewnością",
                ],
            },
        }

    def _build_fallback_context(self, snapshot: dict[str, Any], *, trigger: dict[str, str], cached: dict[str, Any] | None) -> dict[str, Any]:
        bias = _canonical_bias(snapshot.get("decision_bias"))
        conf = _int(snapshot.get("decision_confidence"))
        entry_zone = snapshot.get("entry_zone") if isinstance(snapshot.get("entry_zone"), dict) else {}
        target_zone = snapshot.get("target_zone") if isinstance(snapshot.get("target_zone"), dict) else {}
        trigger_level = _num(snapshot.get("decision_trigger"))
        invalidation = _num(snapshot.get("decision_invalidation"))
        atr = max(0.25, _num(snapshot.get("atr"), 2.0))

        chart_annotations: list[dict[str, Any]] = []
        if entry_zone:
            chart_annotations.append(
                {
                    "label": "LLM watch",
                    "stage": "llm_prediction",
                    "direction": bias,
                    "low": round(_num(entry_zone.get("low")), 2),
                    "high": round(_num(entry_zone.get("high")), 2),
                    "trigger": round(trigger_level, 2) if trigger_level else None,
                    "why": _norm(snapshot.get("decision_summary")),
                }
            )
        if invalidation:
            band = max(0.5, atr * 0.10)
            chart_annotations.append(
                {
                    "label": "LLM invalid",
                    "stage": "llm_invalidation",
                    "direction": "short" if bias == "long" else "long" if bias == "short" else "neutral",
                    "low": round(invalidation - band, 2),
                    "high": round(invalidation + band, 2),
                    "trigger": round(invalidation, 2),
                    "why": "Scenario fails beyond this invalidation.",
                }
            )
        if target_zone:
            chart_annotations.append(
                {
                    "label": "LLM target",
                    "stage": "llm_target",
                    "direction": bias,
                    "low": round(_num(target_zone.get("low")), 2),
                    "high": round(_num(target_zone.get("high")), 2),
                    "trigger": None,
                    "why": "Primary target area for the current scenario.",
                }
            )

        levels_to_watch = []
        for item in list(snapshot.get("top_levels") or [])[:4]:
            levels_to_watch.append(f"{_norm(item.get('label'))} {_num(item.get('price')):.2f}")

        playbook = _norm(snapshot.get("primary_signal_id")) or _norm(snapshot.get("decision_scenario")) or _norm(snapshot.get("daily_bias"))
        warning = ""
        if _norm(snapshot.get("l2_bias")) and bias in {"long", "short"} and _norm(snapshot.get("l2_bias")) not in {bias, "neutral"}:
            warning = f"L2 is leaning {_norm(snapshot.get('l2_bias'))} against the structural bias."
        elif snapshot.get("regime") in {"chop", "transition"}:
            warning = "Regime is rotational; do not chase."
        if snapshot.get("signal_conflict"):
            warning = (warning + " " if warning else "") + "Execution shortlist conflicts with structural bias."

        aligned = _num(snapshot.get("aligned_exec_score"))
        opposite = _num(snapshot.get("opposite_exec_score"))
        alignment_summary = (
            f"Decision {snapshot.get('decision_bias')} vs execution {aligned:.1f}/{opposite:.1f}."
            if snapshot.get("decision_bias") in {"long", "short"}
            else "Decision bias is neutral; wait for alignment."
        )
        conflict_note = "Signals are mixed across directions." if snapshot.get("signal_conflict") else ""
        trade_plan = _norm(snapshot.get("decision_summary"))

        return {
            "bias": bias,
            "confidence": conf,
            "summary": _norm(snapshot.get("decision_summary"), "No intraday summary."),
            "playbook": playbook,
            "warning": warning,
            "alignment_summary": alignment_summary,
            "conflict_note": conflict_note,
            "trade_plan": trade_plan,
            "levels_to_watch": levels_to_watch,
            "chart_annotations": chart_annotations[:4],
            "refresh_reason": trigger["label"],
            "llm_ok": False,
        }

    def _call_llm(self, snapshot: dict[str, Any], *, trigger: dict[str, str]) -> dict[str, Any] | None:
        client = MarketSnapshotLLMClient()
        response = client.call(
            (
                "Jesteś asystentem scenariusza intraday dla MNQ. "
                "Nie analizujesz od zera i nie używasz screenów; pracujesz wyłącznie na structured state. "
                "Zwróć tylko JSON. Pisz po polsku, krótko i egzekucyjnie. "
                "Najpierw HTF, potem 15m/5m struktura, a L2 traktuj tylko jako potwierdzenie. "
                "Jeżeli execution_shortlist, best_long_signal albo best_short_signal są dostępne, traktuj je jako ważniejsze od luźnych top_signals. "
                "Jeżeli decision_bias i execution_shortlist są sprzeczne, nazwij konflikt wprost zamiast wygładzać obraz. "
                "Używaj wyłącznie poziomów cenowych obecnych w snapshotcie. "
                "Nie wymyślaj nowych poziomów. "
                "Jeśli rynek jest mixed, transition albo chop, utrzymuj neutralność i nie udawaj przewagi. "
                "HSB v11.5 RULES: 1. BRAK LENIWYCH STREF (No Lazy Reuses). Zawsze przerysowuj/aktualizuj strefy 'entry' i 'target' tak, aby znajdowały się sensownie blisko aktualnej ceny (Current Price), biorąc pod uwagę ostanie momentum. "
                "2. VOLATILITY SHOCK: Jeżeli 'Powód odświeżenia' wskazuje na VOLATILITY SHOCK, kategorycznie zabrania się otwierania nowych pozycji! Jesteś wstrząśnięty. Bias musi być 'neutral', a stan 'DORMANT' wpisany w summary."
            ),
            (
                "Zwróć JSON z kluczami: "
                "bias, confidence, summary, playbook, warning, alignment_summary, conflict_note, trade_plan, levels_to_watch, chart_annotations. "
                "Dozwolone bias: long, short, neutral, neutral_to_long, neutral_to_short. "
                "summary MUSI być w dokładnym formacie z logów serwera: '-> Micro Decision: [KIERUNEK] | SMC Analysis: [Twoja 2-zdaniowa techniczna analiza struktury]'. "
                "alignment_summary ma mieć 1 krótkie zdanie o zgodności HTF / decision / execution / L2. "
                "conflict_note ma być krótkie i puste, jeżeli nie ma realnego konfliktu. "
                "trade_plan ma być jednym konkretnym zdaniem: co robić, czego nie robić i gdzie jest invalidation. "
                "levels_to_watch mają być krótkimi liniami tekstu opartymi wyłącznie o poziomy z payloadu. "
                "chart_annotations to tablica obiektów z polami: "
                "label, stage, direction, low, high, trigger, why. "
                "Dozwolone stage: llm_prediction, llm_target, llm_invalidation, llm_level. "
                "Jeżeli nie ma czystego edge, wybierz neutral/wait zamiast wymyślać scenariusz.\n\n"
                f"Powód odświeżenia: {trigger['label']}\n\n"
                f"{json.dumps(snapshot, ensure_ascii=True, indent=2, default=str)}"
            ),
            temperature=0.0,
        )
        if not response.success or not isinstance(response.parsed_json, dict):
            if response.error:
                log.info("Intraday LLM skipped: %s", response.error)
            return None
        parsed = response.parsed_json
        refs, max_distance, near_band = _intraday_refs(snapshot)
        current_price = _num(snapshot.get("price"))
        text_fields = [
            _norm(parsed.get("summary")),
            _norm(parsed.get("warning")),
            _norm(parsed.get("alignment_summary")),
            _norm(parsed.get("conflict_note")),
            _norm(parsed.get("trade_plan")),
            *[_norm(x) for x in list(parsed.get("levels_to_watch") or [])[:6]],
        ]
        if not all(_text_prices_are_valid(text, refs, current_price, max_distance, near_band) for text in text_fields):
            log.info("Intraday LLM rejected: invalid price references in text.")
            return None
        chart_annotations = []
        for item in list(parsed.get("chart_annotations") or [])[:5]:
            if not isinstance(item, dict):
                continue
            low = round(_num(item.get("low")), 2)
            high = round(_num(item.get("high")), 2)
            trigger_px = round(_num(item.get("trigger")), 2) if item.get("trigger") is not None else None
            if any(
                value and not _text_prices_are_valid(str(value), refs, current_price, max_distance, near_band)
                for value in (low, high, trigger_px)
            ):
                log.info("Intraday LLM rejected: invalid chart annotation level.")
                return None
            chart_annotations.append(
                {
                    "label": _norm(item.get("label"), "LLM"),
                    "stage": _norm(item.get("stage"), "llm_prediction"),
                    "direction": _norm(item.get("direction"), parsed.get("bias") or "neutral"),
                    "low": low,
                    "high": high,
                    "trigger": trigger_px,
                    "why": _norm(item.get("why"), ""),
                }
            )
        return {
            "bias": _canonical_bias(parsed.get("bias") or snapshot.get("decision_bias")),
            "confidence": _int(parsed.get("confidence"), _int(snapshot.get("decision_confidence"))),
            "summary": _norm(parsed.get("summary"), _norm(snapshot.get("decision_summary"))),
            "playbook": _norm(parsed.get("playbook"), _norm(snapshot.get("primary_signal_id"))),
            "warning": _norm(parsed.get("warning"), ""),
            "alignment_summary": _norm(parsed.get("alignment_summary"), ""),
            "conflict_note": _norm(parsed.get("conflict_note"), ""),
            "trade_plan": _norm(parsed.get("trade_plan"), _norm(snapshot.get("decision_summary"))),
            "levels_to_watch": [_norm(x) for x in list(parsed.get("levels_to_watch") or [])[:6] if _norm(x)],
            "chart_annotations": chart_annotations,
            "llm_ok": True,
            "llm_model": response.model,
        }
