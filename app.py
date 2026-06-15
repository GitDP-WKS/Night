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

# Streamlit Cloud can rerun the whole app after a download click.
# For large Avaya reports this may freeze the frontend or lead to a black screen.
# Make downloads ignore rerun when the installed Streamlit version supports it.
try:
    import streamlit as st

    _original_download_button = st.download_button

    def _download_button_without_rerun(*args, **kwargs):
        kwargs.setdefault("on_click", "ignore")
        try:
            return _original_download_button(*args, **kwargs)
        except TypeError:
            kwargs.pop("on_click", None)
            return _original_download_button(*args, **kwargs)

    st.download_button = _download_button_without_rerun
except Exception:
    pass

from app_shift_unified_v3 import *  # noqa: F401,F403
