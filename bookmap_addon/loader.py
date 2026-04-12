"""Bookmap Python API Loader — paste this into Bookmap's embedded editor.

It loads the full wizjoner_bridge.py addon from disk.
"""
import sys
import os

# Add the addon directory to Python path
addon_dir = "/Users/sacredforest/Trading Setup/SignalDashboard/bookmap_addon"
sys.path.insert(0, addon_dir)

# Load and run the full addon
exec(open(os.path.join(addon_dir, "wizjoner_bridge.py")).read())
