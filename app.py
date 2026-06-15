# app.py
# Entry point for Streamlit Cloud.
# The main analyzer code is stored in app_shift_unified_v3.py.

# Compatibility for newer pandas versions: Styler.applymap was replaced by Styler.map.
try:
    from pandas.io.formats.style import Styler

    if not hasattr(Styler, "applymap") and hasattr(Styler, "map"):
        Styler.applymap = Styler.map
except Exception:
    pass

from app_shift_unified_v3 import *  # noqa: F401,F403
