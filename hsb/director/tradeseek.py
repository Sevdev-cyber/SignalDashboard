"""TradeSeek director — full LLM-based decision making (Option C).

Two-tier system:
1. MACRO PLAN — called every ~15 min to classify the day regime
2. CANDIDATE REVIEW — called for EACH candidate with full context

Falls back to deterministic FallbackDirector if API is unavailable.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd

from hsb.director.client import DeepSeekClient, LLMResponse
from hsb.director.fallback import FallbackDirector
from hsb.director.parser import parse_candidate_review, parse_macro_plan
from hsb.domain.context import AnalysisContext
from hsb.domain.enums import DirectorAction, MacroRegime
from hsb.domain.models import (
    DirectorDecision,
    EventUpdate,
    MacroPlan,
    MicroPlan,
    SignalCandidate,
)

log = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).parent / "prompts"


class TradeSeekDirector:
    """LLM-powered director using DeepSeek API.

    Maintains a cached macro plan and reviews each candidate individually.
    """

    def __init__(
        self,
        client: DeepSeekClient | None = None,
        macro_refresh_minutes: float = 15.0,
    ) -> None:
        self._client = client or DeepSeekClient()
        self._fallback = FallbackDirector()
        self._macro_refresh_sec = macro_refresh_minutes * 60

        # State
        self._cached_macro: MacroPlan | None = None
        self._macro_timestamp: float = 0.0
        self._trade_history: list[dict] = []

        # Load prompts
        self._macro_system = self._load_prompt("macro_plan.txt")
        self._review_system = self._load_prompt("candidate_review.txt")

        # Telemetry callback (set externally)
        self.on_llm_call: list = []

    # ------------------------------------------------------------------
    # Director Protocol implementation
    # ------------------------------------------------------------------

    def decide(
        self,
        candidate: SignalCandidate,
        context: AnalysisContext,
    ) -> DirectorDecision:
        """Review a single candidate — calls LLM with full context."""

        # Ensure macro plan is fresh
        macro = self._ensure_macro(context)

        # Quick deterministic pre-filter (don't waste API calls)
        if candidate.score < 0.35:
            return DirectorDecision(
                action=DirectorAction.BLOCK,
                reason="pre-filter: score < 0.35, not worth LLM review",
            )

        # Check if direction is allowed by macro
        if candidate.direction.value not in macro.allowed_sides:
            return DirectorDecision(
                action=DirectorAction.BLOCK,
                reason=f"macro blocks {candidate.direction.value} — allowed: {macro.allowed_sides}",
            )

        # Build user prompt
        user_prompt = self._build_review_prompt(candidate, context, macro)

        # Call LLM
        response = self._client.call(self._review_system, user_prompt)
        self._log_call("candidate_review", response, candidate.id)

        if not response.success or response.parsed_json is None:
            log.warning("LLM review failed for %s — using fallback", candidate.id)
            return self._fallback.decide(candidate, context)

        return parse_candidate_review(response.parsed_json)

    def macro_plan(self, context: AnalysisContext) -> MacroPlan:
        """Get macro plan — calls LLM or returns cached."""
        return self._ensure_macro(context)

    def micro_plan(self, context: AnalysisContext) -> MicroPlan:
        """Not used in Option C — macro + per-candidate review handles everything."""
        return MicroPlan()

    def on_event(self, event_type: str, context: AnalysisContext) -> EventUpdate:
        """Simple event handling — track trade history for context."""
        return EventUpdate(reason=f"noted: {event_type}")

    def record_trade(self, direction: str, pnl: float, exit_reason: str) -> None:
        """Record a trade outcome for context in future decisions."""
        self._trade_history.append({
            "direction": direction,
            "pnl": round(pnl, 2),
            "exit_reason": exit_reason,
        })
        # Keep last 5
        self._trade_history = self._trade_history[-5:]

    # ------------------------------------------------------------------
    # Macro plan management
    # ------------------------------------------------------------------

    def _ensure_macro(self, context: AnalysisContext) -> MacroPlan:
        """Refresh macro plan if stale (> 15 min old)."""
        now = time.monotonic()
        if self._cached_macro and (now - self._macro_timestamp) < self._macro_refresh_sec:
            return self._cached_macro

        macro = self._call_macro(context)
        self._cached_macro = macro
        self._macro_timestamp = now
        return macro

    def _call_macro(self, context: AnalysisContext) -> MacroPlan:
        """Call LLM for macro plan."""
        user_prompt = self._build_macro_prompt(context)
        response = self._client.call(self._macro_system, user_prompt)
        self._log_call("macro_plan", response, "macro")

        if not response.success or response.parsed_json is None:
            log.warning("LLM macro plan failed — using fallback")
            return self._fallback.macro_plan(context)

        plan = parse_macro_plan(response.parsed_json)
        log.info("LLM Macro: regime=%s bias=%s sides=%s confidence=%.2f (%.1fs)",
                 plan.macro_regime.value, plan.day_bias,
                 plan.allowed_sides, plan.confidence, response.elapsed_sec)
        return plan

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    def _build_macro_prompt(self, ctx: AnalysisContext) -> str:
        """Build user prompt for macro planning — enriched with Kompendium data."""
        bars = ctx.bar_data.bars_df
        last_bars = self._bars_summary(bars, n=6)
        session_window = self._detect_session_window(ctx)
        levels = self._extract_levels(bars)
        flow = self._flow_summary(bars)

        prev_macro = "none"
        if self._cached_macro:
            prev_macro = f"{self._cached_macro.macro_regime.value} (bias={self._cached_macro.day_bias}, confidence={self._cached_macro.confidence:.2f})"

        return f"""Day: {ctx.day}
Session: {ctx.session}
Current time: {ctx.timestamp.isoformat() if ctx.timestamp else "unknown"}
Session window: {session_window}

Day Stats:
- Move from open: {ctx.regime.move_from_open:.1f} pts
- Directional efficiency: {ctx.regime.directional_efficiency:.3f}
- Total path: {ctx.regime.total_path:.1f} pts
- ATR: {ctx.atr:.1f}

Key Levels:
- VWAP: {levels['vwap']:.2f}
- EMA20: {levels['ema20']:.2f}
- EMA50: {levels['ema50']:.2f}
- Prev day high: {levels['pdh']:.2f}
- Prev day low: {levels['pdl']:.2f}
- Current close: {levels['close']:.2f}
- Price vs VWAP: {levels['vs_vwap']}
- Price vs EMA50: {levels['vs_ema50']}

Flow Summary:
{flow}

Last 6 bars:
{last_bars}

Previous macro plan: {prev_macro}

Recent trade history: {self._history_str()}
"""

    def _build_review_prompt(self, c: SignalCandidate, ctx: AnalysisContext, macro: MacroPlan) -> str:
        """Build user prompt for per-candidate review — enriched with Kompendium data."""
        bars = ctx.bar_data.bars_df
        last_bars = self._bars_summary(bars, n=6)
        risk = abs(c.entry_price - c.sl_price)
        rr = abs(c.tp1_price - c.entry_price) / risk if risk > 0 else 0
        rr2 = abs(c.tp2_price - c.entry_price) / risk if risk > 0 else 0
        rr3 = abs(c.tp3_price - c.entry_price) / risk if risk > 0 else 0
        levels = self._extract_levels(bars)
        session_window = self._detect_session_window(ctx)
        flow = self._flow_summary(bars)

        # Entry quality: how far is entry from VWAP?
        vwap_dist_atr = abs(c.entry_price - levels['vwap']) / ctx.atr if ctx.atr > 0 else 0

        return f"""MACRO PLAN:
- Regime: {macro.macro_regime.value}
- Bias: {macro.day_bias}
- Allowed sides: {macro.allowed_sides}
- Risk mode: {macro.risk_mode.value}
- Session quality: {getattr(macro, 'session_quality', 'unknown')}
- Key observation: {getattr(macro, 'key_observation', 'n/a')}

CANDIDATE:
- ID: {c.id}
- Direction: {c.direction.value}
- Family: {c.family.value}
- Entry: {c.entry_price:.2f}
- SL: {c.sl_price:.2f}
- TP1: {c.tp1_price:.2f} (RR={rr:.2f})
- TP2: {c.tp2_price:.2f} (RR={rr2:.2f})
- TP3: {c.tp3_price:.2f} (RR={rr3:.2f})
- Risk: {risk:.2f} pts ({risk/ctx.atr:.2f} × ATR)
- Score: {c.score:.2f}
- Confluence reasons: {', '.join(c.reasons)} ({len(c.reasons)} factors)

MARKET CONTEXT:
- ATR: {ctx.atr:.1f}
- Move from open: {ctx.regime.move_from_open:.1f} pts
- Efficiency: {ctx.regime.directional_efficiency:.3f}
- Session window: {session_window}
- Position: {ctx.position.raw}

STRUCTURE:
- VWAP: {levels['vwap']:.2f} (entry is {vwap_dist_atr:.2f}×ATR from VWAP)
- EMA20: {levels['ema20']:.2f}
- EMA50: {levels['ema50']:.2f}
- Prev day high: {levels['pdh']:.2f}
- Prev day low: {levels['pdl']:.2f}
- Price vs VWAP: {levels['vs_vwap']}
- Price vs EMA50: {levels['vs_ema50']}

FLOW:
{flow}

Last 6 bars:
{last_bars}

Recent trades:
{self._history_str()}
"""

    # ------------------------------------------------------------------
    # Data extraction helpers
    # ------------------------------------------------------------------

    def _bars_summary(self, bars: pd.DataFrame, n: int = 6) -> str:
        if bars.empty:
            return "no bars available"
        tail = bars.tail(n)
        lines = []
        for _, row in tail.iterrows():
            ts = str(row.get("timestamp", ""))[:19]
            o = f"{float(row.get('open', 0)):.2f}"
            h = f"{float(row.get('high', 0)):.2f}"
            lo = f"{float(row.get('low', 0)):.2f}"
            c = f"{float(row.get('close', 0)):.2f}"
            v = int(row.get("volume", 0))
            d = f"{float(row.get('delta', 0)):.0f}"
            cvd = f"{float(row.get('cvd', 0)):.0f}" if "cvd" in row.index else "?"
            lines.append(f"  {ts} O={o} H={h} L={lo} C={c} Vol={v} Delta={d} CVD={cvd}")
        return "\n".join(lines)

    def _detect_session_window(self, ctx: AnalysisContext) -> str:
        """Classify current time into session window."""
        if ctx.timestamp is None:
            return "unknown"
        hour = ctx.timestamp.hour
        minute = ctx.timestamp.minute
        t = hour * 60 + minute  # minutes since midnight UTC

        # Assuming US Eastern: market open 9:30 = 14:30 UTC, close 16:00 = 21:00 UTC
        if t < 14 * 60 + 30:
            return "premarket"
        elif t < 15 * 60:
            return "cash_open"
        elif t < 16 * 60 + 30:
            return "morning"
        elif t < 18 * 60:
            return "lunch_lull"
        elif t < 19 * 60 + 30:
            return "afternoon"
        elif t < 20 * 60 + 30:
            return "power_hour"
        else:
            return "closing"

    def _extract_levels(self, bars: pd.DataFrame) -> dict:
        """Extract key levels from bar data."""
        if bars.empty:
            return {"vwap": 0, "ema20": 0, "ema50": 0, "pdh": 0, "pdl": 0, "close": 0, "vs_vwap": "?", "vs_ema50": "?"}

        last = bars.iloc[-1]
        close = float(last.get("close", 0))
        vwap = float(last.get("vwap", 0))
        ema20 = float(last.get("ema_20", 0))
        ema50 = float(last.get("ema_50", 0))
        pdh = float(last.get("prev_day_high", 0))
        pdl = float(last.get("prev_day_low", 0))

        vs_vwap = "above" if close > vwap and vwap > 0 else ("below" if vwap > 0 else "n/a")
        vs_ema50 = "above" if close > ema50 and ema50 > 0 else ("below" if ema50 > 0 else "n/a")

        return {"vwap": vwap, "ema20": ema20, "ema50": ema50, "pdh": pdh, "pdl": pdl, "close": close, "vs_vwap": vs_vwap, "vs_ema50": vs_ema50}

    def _flow_summary(self, bars: pd.DataFrame) -> str:
        """Interpret flow — not just numbers but meaning."""
        if bars.empty or "delta" not in bars.columns:
            return "  no flow data available"

        last_6 = bars.tail(6)
        deltas = pd.to_numeric(last_6["delta"], errors="coerce").dropna()
        closes = pd.to_numeric(last_6["close"], errors="coerce").dropna()

        if len(deltas) < 2 or len(closes) < 2:
            return "  insufficient flow data"

        net_delta = float(deltas.sum())
        price_change = float(closes.iloc[-1] - closes.iloc[0])
        avg_delta = float(deltas.mean())

        # Delta effectiveness: is buying/selling actually moving price?
        if abs(net_delta) > 50 and abs(price_change) < 3:
            effectiveness = "LOW (high delta but price not moving → possible absorption)"
        elif abs(net_delta) < 20 and abs(price_change) > 15:
            effectiveness = "VACUUM (price moving without delta → liquidity gap)"
        else:
            effectiveness = "NORMAL (delta and price aligned)"

        # CVD interpretation
        cvd_str = "n/a"
        if "cvd" in bars.columns:
            cvd_last = pd.to_numeric(last_6["cvd"], errors="coerce").dropna()
            if len(cvd_last) >= 2:
                cvd_change = float(cvd_last.iloc[-1] - cvd_last.iloc[0])
                if cvd_change > 30 and price_change < -5:
                    cvd_str = f"BEARISH DIVERGENCE (CVD rising +{cvd_change:.0f} but price falling {price_change:.1f})"
                elif cvd_change < -30 and price_change > 5:
                    cvd_str = f"BULLISH DIVERGENCE (CVD falling {cvd_change:.0f} but price rising +{price_change:.1f})"
                elif cvd_change > 0:
                    cvd_str = f"confirming buyers (+{cvd_change:.0f})"
                else:
                    cvd_str = f"confirming sellers ({cvd_change:.0f})"

        # Pressure direction
        if avg_delta > 10:
            pressure = "buying pressure dominant"
        elif avg_delta < -10:
            pressure = "selling pressure dominant"
        else:
            pressure = "balanced / no clear pressure"

        return f"""  - Net delta (6 bars): {net_delta:.0f}
  - Price change (6 bars): {price_change:.2f} pts
  - Avg delta per bar: {avg_delta:.1f}
  - Delta effectiveness: {effectiveness}
  - CVD: {cvd_str}
  - Pressure: {pressure}"""

    def _history_str(self) -> str:
        if not self._trade_history:
            return "none (first trade of session)"
        lines = []
        for t in self._trade_history[-3:]:
            lines.append(f"  - {t['direction']} PnL=${t['pnl']:.2f} exit={t['exit_reason']}")
        # Add summary
        recent_pnl = sum(t['pnl'] for t in self._trade_history[-3:])
        recent_wr = sum(1 for t in self._trade_history[-3:] if t['pnl'] > 0)
        lines.append(f"  Session so far: {len(self._trade_history)} trades, net ${recent_pnl:.2f}")
        return "\n".join(lines)

    def _load_prompt(self, filename: str) -> str:
        path = _PROMPTS_DIR / filename
        if path.exists():
            return path.read_text(encoding="utf-8")
        log.warning("Prompt not found: %s — using empty", path)
        return ""

    def _log_call(self, call_type: str, response: LLMResponse, context_id: str) -> None:
        payload = {
            "type": call_type,
            "context_id": context_id,
            "success": response.success,
            "elapsed_sec": response.elapsed_sec,
            "model": response.model,
            "error": response.error,
        }
        for cb in self.on_llm_call:
            cb(payload)
