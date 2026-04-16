from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PlaybookSpec:
    playbook_id: str
    title: str
    tier: str
    priority: str
    ready_state: str
    tradeable: bool
    signal_kinds: tuple[str, ...] = field(default_factory=tuple)
    allowed_regimes: tuple[str, ...] = field(default_factory=tuple)
    blocked_regimes: tuple[str, ...] = field(default_factory=tuple)
    session_windows_et: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    require_reasons_any: tuple[str, ...] = field(default_factory=tuple)
    exclude_reasons_any: tuple[str, ...] = field(default_factory=tuple)
    require_source_tokens_any: tuple[str, ...] = field(default_factory=tuple)
    exclude_source_tokens_any: tuple[str, ...] = field(default_factory=tuple)
    min_signal_confidence: int = 70
    min_cluster: int = 2
    min_decision_confidence: int = 66
    max_entry_dist_atr: float = 0.75
    max_initial_risk_points: float = 20.0
    min_target_r_multiple: float = 0.25
    max_pending_bars: int = 3
    max_position_bars: int = 18
    be_progress_r: float = 0.45
    lock_progress_r: float = 0.90
    soft_exit_progress_r: float = 0.40
    break_even_buffer: float = 0.25
    lock_profit_r: float = 0.35
    selection_bonus: float = 0.0
    notes: str = ""


PLAYBOOK_SPECS: tuple[PlaybookSpec, ...] = (
    PlaybookSpec(
        playbook_id="PB01",
        title="NY Open Drive Continuation",
        tier="core",
        priority="A+",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_mtf_confluence", "ns_break_retest_pro", "ns_macro_swing"),
        allowed_regimes=("trend_up", "trend_down", "transition"),
        session_windows_et=(("09:30", "10:05"),),
        require_reasons_any=("trend_aligned", "score=4", "score=5", "ema_reject", "ema_pullback"),
        exclude_reasons_any=("reversal_setup",),
        min_signal_confidence=74,
        min_cluster=2,
        min_decision_confidence=68,
        max_entry_dist_atr=0.55,
        max_initial_risk_points=18.0,
        min_target_r_multiple=0.35,
        max_pending_bars=2,
        max_position_bars=14,
        be_progress_r=0.40,
        lock_progress_r=0.85,
        soft_exit_progress_r=0.35,
        break_even_buffer=0.25,
        lock_profit_r=0.30,
        selection_bonus=7.0,
        notes="Opening drive. First continuation or first accepted retest only.",
    ),
    PlaybookSpec(
        playbook_id="PB02",
        title="5m ORB Break + Retest",
        tier="core",
        priority="A",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_break_retest_pro", "ns_mtf_confluence", "ns_ib_break"),
        allowed_regimes=("trend_up", "trend_down", "transition"),
        session_windows_et=(("09:35", "10:15"),),
        require_reasons_any=("trend_aligned", "score=4", "score=5", "ib_break", "displacement_break"),
        exclude_reasons_any=("reversal_setup",),
        min_signal_confidence=76,
        min_cluster=2,
        min_decision_confidence=68,
        max_entry_dist_atr=0.50,
        max_initial_risk_points=16.0,
        min_target_r_multiple=0.40,
        max_pending_bars=2,
        max_position_bars=16,
        be_progress_r=0.45,
        lock_progress_r=0.90,
        soft_exit_progress_r=0.40,
        break_even_buffer=0.25,
        lock_profit_r=0.35,
        selection_bonus=8.0,
        notes="ORB / break-retest. No extension chase; entry must still be near retest zone.",
    ),
    PlaybookSpec(
        playbook_id="PB03",
        title="HTF Pullback to Value Continuation",
        tier="core",
        priority="A+",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_break_retest_pro", "ns_ifvg_reclaim", "ns_macro_swing", "ns_mtf_confluence"),
        allowed_regimes=("trend_up", "trend_down"),
        session_windows_et=(("09:30", "11:30"), ("13:00", "15:00")),
        require_reasons_any=("trend_aligned", "inversion_hold", "score=4", "score=5", "ema_pullback", "ema_reject"),
        exclude_reasons_any=("reversal_setup",),
        min_signal_confidence=72,
        min_cluster=2,
        min_decision_confidence=66,
        max_entry_dist_atr=0.75,
        max_initial_risk_points=20.0,
        min_target_r_multiple=0.30,
        max_pending_bars=3,
        max_position_bars=22,
        be_progress_r=0.50,
        lock_progress_r=1.00,
        soft_exit_progress_r=0.45,
        break_even_buffer=0.25,
        lock_profit_r=0.40,
        selection_bonus=6.0,
        notes="HTF-aligned pullback to value. Wider hold allowed, but only with trend regime.",
    ),
    PlaybookSpec(
        playbook_id="PB04",
        title="IFVG Reclaim Continuation",
        tier="core",
        priority="A",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_ifvg_reclaim",),
        allowed_regimes=("trend_up", "trend_down", "transition"),
        session_windows_et=(("09:30", "11:30"), ("13:00", "15:00")),
        require_reasons_any=("inversion_hold", "gap_size=", "delta_aligned"),
        require_source_tokens_any=("ifvg",),
        exclude_reasons_any=("reversal_setup",),
        min_signal_confidence=74,
        min_cluster=2,
        min_decision_confidence=66,
        max_entry_dist_atr=0.60,
        max_initial_risk_points=16.0,
        min_target_r_multiple=0.35,
        max_pending_bars=2,
        max_position_bars=18,
        be_progress_r=0.45,
        lock_progress_r=0.90,
        soft_exit_progress_r=0.40,
        break_even_buffer=0.25,
        lock_profit_r=0.35,
        selection_bonus=7.0,
        notes="IFVG reclaim only after hold. No blind box limit fills.",
    ),
    PlaybookSpec(
        playbook_id="PB05",
        title="VWAP Trend Reclaim",
        tier="core",
        priority="A-",
        ready_state="partial",
        tradeable=True,
        signal_kinds=("ns_vwap_mean_reversion",),
        allowed_regimes=("trend_up", "trend_down"),
        session_windows_et=(("09:40", "11:00"), ("13:30", "15:00")),
        require_reasons_any=("vwap", "accept_above_vwap", "accept_below_vwap", "vwap_reclaim", "vwap_loss"),
        require_source_tokens_any=("vwap",),
        exclude_reasons_any=("reversal_setup",),
        min_signal_confidence=73,
        min_cluster=2,
        min_decision_confidence=66,
        max_entry_dist_atr=0.55,
        max_initial_risk_points=14.0,
        min_target_r_multiple=0.30,
        max_pending_bars=2,
        max_position_bars=14,
        be_progress_r=0.40,
        lock_progress_r=0.85,
        soft_exit_progress_r=0.35,
        break_even_buffer=0.25,
        lock_profit_r=0.30,
        selection_bonus=5.0,
        notes="Trend-side VWAP reclaim only, not generic first-touch mean reversion.",
    ),
    PlaybookSpec(
        playbook_id="PB06",
        title="Liquidity Sweep + Reclaim Reversal",
        tier="secondary",
        priority="B+",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_macro_swing", "ns_absorption_sweep"),
        allowed_regimes=("range", "chop", "transition", "trend_up", "trend_down"),
        session_windows_et=(("09:45", "11:00"), ("14:30", "16:00")),
        require_reasons_any=("high_sweep", "low_sweep", "rejection", "sell_absorption", "buy_absorption", "ema_reject"),
        min_signal_confidence=74,
        min_cluster=2,
        min_decision_confidence=66,
        max_entry_dist_atr=0.60,
        max_initial_risk_points=16.0,
        min_target_r_multiple=0.30,
        selection_bonus=3.0,
        notes="Session extreme reversal only.",
    ),
    PlaybookSpec(
        playbook_id="PB07",
        title="Failed Breakout / Breakdown Flip",
        tier="secondary",
        priority="B",
        ready_state="partial",
        tradeable=True,
        signal_kinds=("ns_macro_swing", "ns_absorption_sweep", "ns_break_retest_pro"),
        allowed_regimes=("range", "chop", "transition"),
        session_windows_et=(("09:35", "11:00"), ("13:00", "14:30")),
        require_reasons_any=("rejection", "ema_reject", "failed_breakout", "failed_breakdown"),
        min_signal_confidence=72,
        min_cluster=2,
        min_decision_confidence=64,
        max_entry_dist_atr=0.65,
        max_initial_risk_points=17.0,
        min_target_r_multiple=0.25,
        selection_bonus=2.0,
        notes="Needs a dedicated failure tag for full production confidence.",
    ),
    PlaybookSpec(
        playbook_id="PB08",
        title="Midday Rotational VWAP Reversion",
        tier="secondary",
        priority="B+",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_vwap_mean_reversion",),
        allowed_regimes=("range", "chop", "transition"),
        session_windows_et=(("11:00", "13:30"),),
        require_source_tokens_any=("vwap",),
        min_signal_confidence=72,
        min_cluster=2,
        min_decision_confidence=64,
        max_entry_dist_atr=0.55,
        max_initial_risk_points=12.0,
        min_target_r_multiple=0.25,
        max_position_bars=12,
        selection_bonus=2.0,
        notes="Lunch rotation only.",
    ),
    PlaybookSpec(
        playbook_id="PB09",
        title="Post-Lunch Rebuild Continuation",
        tier="secondary",
        priority="B",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_mtf_confluence", "ns_break_retest_pro"),
        allowed_regimes=("trend_up", "trend_down", "transition"),
        session_windows_et=(("13:00", "14:30"),),
        require_reasons_any=("trend_aligned", "score=4", "score=5"),
        min_signal_confidence=72,
        min_cluster=2,
        min_decision_confidence=66,
        max_entry_dist_atr=0.65,
        max_initial_risk_points=18.0,
        min_target_r_multiple=0.30,
        max_position_bars=18,
        selection_bonus=2.5,
        notes="Rebuild after midday balance.",
    ),
    PlaybookSpec(
        playbook_id="PB10",
        title="Power Hour Continuation",
        tier="secondary",
        priority="B",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_mtf_confluence", "ns_break_retest_pro", "ns_macro_swing"),
        allowed_regimes=("trend_up", "trend_down"),
        session_windows_et=(("15:00", "16:00"),),
        require_reasons_any=("trend_aligned", "score=4", "score=5", "ema_reject"),
        min_signal_confidence=74,
        min_cluster=2,
        min_decision_confidence=68,
        max_entry_dist_atr=0.60,
        max_initial_risk_points=18.0,
        min_target_r_multiple=0.30,
        max_position_bars=12,
        selection_bonus=2.5,
        notes="Only when day trend already exists.",
    ),
    PlaybookSpec(
        playbook_id="PB11",
        title="London Close Fade / Mid-Morning Reversion",
        tier="specialist",
        priority="C+",
        ready_state="partial",
        tradeable=True,
        signal_kinds=("ns_macro_swing", "ns_vwap_mean_reversion"),
        allowed_regimes=("range", "chop", "transition"),
        session_windows_et=(("10:00", "12:00"),),
        require_reasons_any=("rejection", "ema_reject"),
        min_signal_confidence=72,
        min_cluster=2,
        min_decision_confidence=64,
        max_entry_dist_atr=0.65,
        max_initial_risk_points=14.0,
        min_target_r_multiple=0.25,
        selection_bonus=1.0,
        notes="Needs London-close specific context to be primary.",
    ),
    PlaybookSpec(
        playbook_id="PB12",
        title="News Dislocation Second Leg",
        tier="specialist",
        priority="C+",
        ready_state="needs_extra_context",
        tradeable=False,
        signal_kinds=("ns_macro_swing", "ns_mtf_confluence", "ns_absorption_sweep"),
        allowed_regimes=("trend_up", "trend_down", "transition"),
        min_signal_confidence=78,
        min_cluster=2,
        min_decision_confidence=70,
        max_entry_dist_atr=0.70,
        max_initial_risk_points=24.0,
        min_target_r_multiple=0.35,
        selection_bonus=1.0,
        notes="Requires macro event join and explicit news regime labeling.",
    ),
    PlaybookSpec(
        playbook_id="PB13",
        title="IB Break Continuation",
        tier="specialist",
        priority="C",
        ready_state="ready",
        tradeable=True,
        signal_kinds=("ns_ib_break",),
        allowed_regimes=("trend_up", "trend_down", "transition"),
        session_windows_et=(("09:45", "11:00"),),
        require_reasons_any=("ib_break", "above_ib", "below_ib"),
        min_signal_confidence=76,
        min_cluster=1,
        min_decision_confidence=66,
        max_entry_dist_atr=0.55,
        max_initial_risk_points=16.0,
        min_target_r_multiple=0.35,
        selection_bonus=3.5,
        notes="Discrete IB playbook, but low current frequency.",
    ),
)


CORE_PLAYBOOK_IDS: tuple[str, ...] = ("PB01", "PB02", "PB03", "PB04", "PB05")
PLAYBOOK_BY_ID = {pb.playbook_id: pb for pb in PLAYBOOK_SPECS}
PLAYBOOK_ORDER = {pb.playbook_id: idx for idx, pb in enumerate(PLAYBOOK_SPECS)}
KNOWN_SIGNAL_KINDS = tuple(
    sorted(
        {
            signal_kind
            for playbook in PLAYBOOK_SPECS
            for signal_kind in playbook.signal_kinds
        },
        key=len,
        reverse=True,
    )
)


def get_tradeable_playbooks() -> tuple[PlaybookSpec, ...]:
    return tuple(pb for pb in PLAYBOOK_SPECS if pb.tradeable)


def get_playbook_spec(playbook_id: str) -> PlaybookSpec | None:
    return PLAYBOOK_BY_ID.get(str(playbook_id or "").upper())


def _parse_reasons(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except Exception:
            pass
    return [text]


def _joined_lower(parts: list[str]) -> str:
    return " | ".join(str(x) for x in parts).lower()


def _get_signal_kind(sig: dict[str, Any]) -> str:
    direct = str(sig.get("lead_signal_kind") or sig.get("signal_kind") or "").lower()
    if direct:
        return direct
    for field_name in ("source_type", "name"):
        text = str(sig.get(field_name) or "").lower()
        for signal_kind in KNOWN_SIGNAL_KINDS:
            if signal_kind in text:
                return signal_kind
    return ""


def _get_source_type(sig: dict[str, Any]) -> str:
    return str(sig.get("source_type") or "").lower()


def _get_regime(value: Any) -> str:
    return str(value or "").strip().lower()


def _hhmm_from_any(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%H:%M")
        except Exception:
            pass
    text = str(value).strip()
    if len(text) >= 16 and "T" in text:
        return text[11:16]
    if len(text) >= 5 and text[2] == ":":
        return text[:5]
    return ""


def _in_windows(hhmm: str, windows: tuple[tuple[str, str], ...]) -> bool:
    if not windows:
        return True
    if not hhmm:
        return False
    return any(start <= hhmm <= end for start, end in windows)


def _contains_any(haystack_text: str, needles: tuple[str, ...]) -> bool:
    if not needles:
        return True
    return any(needle.lower() in haystack_text for needle in needles)


def signal_matches_playbook(
    sig: dict[str, Any],
    playbook: PlaybookSpec,
    *,
    regime: str,
    timestamp_et: Any,
) -> bool:
    if not playbook.tradeable:
        return False
    signal_kind = _get_signal_kind(sig)
    if signal_kind not in playbook.signal_kinds:
        return False

    regime_text = _get_regime(regime or sig.get("regime"))
    if playbook.allowed_regimes and regime_text not in playbook.allowed_regimes:
        return False
    if playbook.blocked_regimes and regime_text in playbook.blocked_regimes:
        return False

    hhmm = _hhmm_from_any(timestamp_et or sig.get("timestamp") or sig.get("bar_time"))
    if playbook.session_windows_et and not _in_windows(hhmm, playbook.session_windows_et):
        return False

    reasons = _parse_reasons(sig.get("reasons_list") or sig.get("reasons"))
    reason_text = _joined_lower(reasons)
    source_text = _get_source_type(sig)
    if playbook.require_reasons_any and not _contains_any(reason_text, playbook.require_reasons_any):
        return False
    if playbook.exclude_reasons_any and _contains_any(reason_text, playbook.exclude_reasons_any):
        return False
    if playbook.require_source_tokens_any and not _contains_any(source_text, playbook.require_source_tokens_any):
        return False
    if playbook.exclude_source_tokens_any and _contains_any(source_text, playbook.exclude_source_tokens_any):
        return False
    return True


def classify_signal_playbooks(
    sig: dict[str, Any],
    *,
    regime: str,
    timestamp_et: Any,
    allowed_playbooks: tuple[str, ...] = (),
) -> tuple[PlaybookSpec, ...]:
    allowed = {item.upper() for item in allowed_playbooks if str(item).strip()} if allowed_playbooks else None
    matched: list[PlaybookSpec] = []
    for playbook in get_tradeable_playbooks():
        if allowed is not None and playbook.playbook_id not in allowed:
            continue
        if signal_matches_playbook(sig, playbook, regime=regime, timestamp_et=timestamp_et):
            matched.append(playbook)
    matched.sort(key=lambda pb: PLAYBOOK_ORDER[pb.playbook_id])
    return tuple(matched)
