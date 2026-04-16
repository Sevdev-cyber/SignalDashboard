"""Preset selector bot that trades only the core PB01-PB05 playbooks.

This module does not replace ``SignalExecutionBot``. It freezes a cleaner
execution preset for the next iteration, where the selector should only work
with the curated core playbook catalog instead of the broader mixed signal pool.
"""

from __future__ import annotations

from dataclasses import dataclass

from signal_execution_bot import ExecutionConfig, SignalExecutionBot


@dataclass(slots=True)
class CorePlaybookSelectorConfig:
    allowed_playbooks: tuple[str, ...] = ("PB01", "PB02", "PB03", "PB04", "PB05")
    session_trade_cap: int = 8
    session_direction_cap: int = 5
    family_cooldown_win_bars: int = 10
    family_cooldown_loss_bars: int = 18
    zone_cooldown_bars: int = 18
    zone_reuse_points: float = 6.0
    hard_flip_confidence: int = 82
    soft_flip_confidence: int = 74


def build_core_playbook_selector(
    config: CorePlaybookSelectorConfig | None = None,
) -> SignalExecutionBot:
    cfg = config or CorePlaybookSelectorConfig()
    exec_cfg = ExecutionConfig(
        allowed_playbooks=cfg.allowed_playbooks,
        allow_legacy_fallback=False,
        session_trade_cap=cfg.session_trade_cap,
        session_direction_cap=cfg.session_direction_cap,
        family_cooldown_win_bars=cfg.family_cooldown_win_bars,
        family_cooldown_loss_bars=cfg.family_cooldown_loss_bars,
        zone_cooldown_bars=cfg.zone_cooldown_bars,
        zone_reuse_points=cfg.zone_reuse_points,
        hard_flip_confidence=cfg.hard_flip_confidence,
        soft_flip_confidence=cfg.soft_flip_confidence,
    )
    return SignalExecutionBot(exec_cfg)
