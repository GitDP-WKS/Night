# app.py
# Stable Streamlit Cloud entrypoint for Avaya CMS shift analysis.

from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


st.set_page_config(page_title="Avaya CMS: анализ смен", layout="wide")

SETTINGS_FILE = Path("avaya_shift_settings.json")
MAX_TABLE_ROWS = 300
MAX_EXPORT_EVENTS_IN_XLSX = 5000

DEFAULT_AGENT_MAP: Dict[str, str] = {
    "7599449": "Абусаитов ДМ",
    "7599415": "Аспенбетова АА",
    "7599497": "Ахметзянова РР",
    "7599437": "Воронцов ВВ",
    "7599458": "Гайфуллина ДИ",
    "7599473": "Галиева АР",
    "7599413": "Гараев РР",
    "7599498": "Заббаров АИ",
    "7599405": "Зайнудтинова ЛС",
    "7599411": "Ибрагимова ЛИ",
    "7599403": "Минибаева АИ",
    "7599408": "Сагетдинов МИ",
    "7599478": "Хузахметов АР",
}

DEFAULT_SKILL_NAMES: Dict[str, str] = {
    "1": "Надежность",
    "3": "Качество э/э",
    "9": "ЭЗС",
}

DISP_ACCEPTED = {"ANS"}
DISP_MISSED = {"ABAN"}


@dataclass(frozen=True)
class ShiftSpec:
    name: str
    start_hm: Tuple[int, int]
    end_hm: Tuple[int, int]
    crosses_midnight: Optional[bool] = None

    def bounds(self, base_date: date) -> Tuple[datetime, datetime]:
        start_dt = datetime.combine(base_date, time(self.start_hm[0], self.start_hm[1]))
        end_dt = datetime.combine(base_date, time(self.end_hm[0], self.end_hm[1]))
        crosses = (end_dt <= start_dt) if self.crosses_midnight is None else self.crosses_midnight
        if crosses:
            end_dt += timedelta(days=1)
        return start_dt, end_dt


SHIFT_PRESETS = {
    "night": ShiftSpec("Ночная (18:30-06:30)", (18, 30), (6, 30), None),
    "day": ShiftSpec("Дневная (06:30-18:30)", (6, 30), (18, 30), False),
    "custom": ShiftSpec("Кастом", (18, 30), (6, 30), None),
}


def safe_download_button(*args, **kwargs):
    kwargs.setdefault("on_click", "ignore")
    try:
        return st.download_button(*args, **kwargs)
    except TypeError:
        kwargs.pop("on_click", None)
        return st.download_button(*args, **kwargs)


def clean_text(value) -> str:
    if value is None:
        return ""
    value = str(value).replace("\xa0", " ").strip()
    if value.lower() in {"nan", "none", "&nbsp;"}:
        return ""
    return value


def safe_decode(data: bytes) -> str:
    for enc in ("cp1251", "windows-1251", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def first_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    low_map = {str(c).lower().strip(): c for c in df.columns}
    for col in candidates:
        hit = low_map.get(col.lower().strip())
        if hit is not None:
            return hit
    return None


def find_target_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None

    def score(table) -> int:
        headers = [clean_text(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers:
            return 0
        points = 0
        for key in ("id вызова", "размещение", "split/skill", "имена пользователей", "время нач", "date", "disposition"):
            if any(key in h for h in headers):
                points += 3
        points += min(len(headers), 40) // 4
        return points

    best = max(tables, key=score)
    return best if score(best) > 0 else None


@st.cache_data(show_spinner=False, max_entries=2)
def parse_avaya_html(file_bytes: bytes) -> pd.DataFrame:
    text = safe_decode(file_bytes)
    soup = BeautifulSoup(text, "lxml")
    table = find_target_table(soup)
    if table is None:
        return pd.DataFrame()

    headers = [clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    if not headers:
        return pd.DataFrame()

    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row = [clean_text(td.get_text(" ", strip=True)) for td in cells]
        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[: len(headers)]
        rows.append(row)

    return pd.DataFrame(rows, columns=headers)


def detect_columns(df_raw: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {
        "Дата": first_existing_col(df_raw, ["Дата", "Date"]),
        "Время нач.": first_existing_col(df_raw, ["Время нач.", "Время нач", "Time"]),
        "Размещение": first_existing_col(df_raw, ["Размещение", "Disposition"]),
        "Split/Skill": first_existing_col(df_raw, ["Split/Skill", "Skill", "Split"]),
        "Оператор": first_existing_col(df_raw, ["Имена пользователей", "Agent", "Пользователь", "User"]),
    }


def normalize_calls(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Optional[str]], List[str]]:
    if df_raw.empty:
        return pd.DataFrame(), {}, ["HTML-таблица не найдена"]

    found = detect_columns(df_raw)
    missing = [name for name in ("Дата", "Время нач.", "Размещение") if not found.get(name)]
    if missing:
        return pd.DataFrame(), found, missing

    df = df_raw.copy()
    dt_text = (df[found["Дата"]].astype(str).str.strip() + " " + df[found["Время нач."]].astype(str).str.strip()).str.strip()
    df["dt_start"] = pd.to_datetime(dt_text, format="%d.%m.%Y %H:%M:%S", errors="coerce")
    if df["dt_start"].isna().all():
        df["dt_start"] = pd.to_datetime(dt_text, dayfirst=True, errors="coerce")
    df = df.dropna(subset=["dt_start"]).reset_index(drop=True)

    if df.empty:
        return pd.DataFrame(), found, ["Дата/время не распознаны"]

    df["disposition"] = df[found["Размещение"]].astype(str).str.strip().str.upper()
    df["skill_raw"] = "" if found["Split/Skill"] is None else df[found["Split/Skill"]].astype(str).str.strip()
    df["agent_code"] = "" if found["Оператор"] is None else df[found["Оператор"]].astype(str).str.strip()
    df["agent_code"] = df["agent_code"].replace({"nan": "", "None": ""})
    df["skill_code"] = df["skill_raw"].str.extract(r"(\d+)", expand=False).fillna("").astype(str)
    df["call_date"] = df["dt_start"].dt.date
    df["slot"] = df["dt_start"].apply(lambda x: pd.Timestamp(x).replace(minute=(0 if x.minute < 30 else 30), second=0, microsecond=0))

    return df, found, []


def bool_value(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return True
    return str(value).strip().lower() not in {"0", "false", "нет", "no", "inactive", "неактивен"}


def default_agent_table() -> pd.DataFrame:
    return pd.DataFrame([{"agent_code": c, "agent_name": n, "active": True} for c, n in sorted(DEFAULT_AGENT_MAP.items())])


def default_skill_table() -> pd.DataFrame:
    return pd.DataFrame([{"skill_code": c, "skill_name": n, "watched": True} for c, n in sorted(DEFAULT_SKILL_NAMES.items())])


def normalize_agent_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return default_agent_table()
    out = df.copy()
    for col in ("agent_code", "agent_name", "active"):
        if col not in out.columns:
            out[col] = True if col == "active" else ""
    out = out[["agent_code", "agent_name", "active"]].fillna("")
    out["agent_code"] = out["agent_code"].astype(str).str.strip()
    out["agent_name"] = out["agent_name"].astype(str).str.strip()
    out["active"] = out["active"].apply(bool_value)
    out = out[(out["agent_code"] != "") & (out["agent_name"] != "")]
    return out.drop_duplicates("agent_code", keep="last").sort_values("agent_code").reset_index(drop=True)


def normalize_skill_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return default_skill_table()
    out = df.copy()
    for col in ("skill_code", "skill_name", "watched"):
        if col not in out.columns:
            out[col] = True if col == "watched" else ""
    out = out[["skill_code", "skill_name", "watched"]].fillna("")
    out["skill_code"] = out["skill_code"].astype(str).str.extract(r"(\d+)", expand=False).fillna("").astype(str)
    out["skill_name"] = out["skill_name"].astype(str).str.strip()
    out["watched"] = out["watched"].apply(bool_value)
    out = out[out["skill_code"] != ""]
    out.loc[out["skill_name"] == "", "skill_name"] = out["skill_code"]
    return out.drop_duplicates("skill_code", keep="last").sort_values("skill_code").reset_index(drop=True)


def load_settings() -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not SETTINGS_FILE.exists():
        return default_agent_table(), default_skill_table()
    try:
        payload = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        return normalize_agent_table(pd.DataFrame(payload.get("agents", []))), normalize_skill_table(pd.DataFrame(payload.get("skills", [])))
    except Exception:
        return default_agent_table(), default_skill_table()


def save_settings(agent_table: pd.DataFrame, skill_table: pd.DataFrame) -> Tuple[bool, str]:
    try:
        payload = {
            "agents": normalize_agent_table(agent_table).to_dict(orient="records"),
            "skills": normalize_skill_table(skill_table).to_dict(orient="records"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, "Настройки сохранены"
    except Exception as exc:
        return False, f"Не удалось сохранить настройки: {exc}"


def load_agent_file(uploaded) -> pd.DataFrame:
    if uploaded is None:
        return pd.DataFrame(columns=["agent_code", "agent_name", "active"])
    try:
        data = uploaded.getvalue()
        name = uploaded.name.lower()
        if name.endswith((".csv", ".txt")):
            df = None
            for sep in (";", ",", "\t"):
                try:
                    candidate = pd.read_csv(io.BytesIO(data), sep=sep, dtype=str)
                    if candidate.shape[1] >= 2:
                        df = candidate
                        break
                except Exception:
                    pass
            if df is None:
                return pd.DataFrame(columns=["agent_code", "agent_name", "active"])
        else:
            df = pd.read_excel(io.BytesIO(data), dtype=str)

        lower = [str(c).lower().strip() for c in df.columns]
        code_col = name_col = active_col = None
        for norm, raw in zip(lower, df.columns):
            if code_col is None and any(k in norm for k in ("agent", "code", "номер", "id", "код")):
                code_col = raw
            if name_col is None and any(k in norm for k in ("name", "имя", "фио", "оператор")):
                name_col = raw
            if active_col is None and any(k in norm for k in ("active", "актив", "статус")):
                active_col = raw
        if code_col is None or name_col is None:
            code_col, name_col = df.columns[0], df.columns[1]

        rows = []
        for _, row in df.fillna("").iterrows():
            code = str(row[code_col]).strip()
            name = str(row[name_col]).strip()
            if code and name:
                rows.append({"agent_code": code, "agent_name": name, "active": bool_value(row[active_col]) if active_col else True})
        return normalize_agent_table(pd.DataFrame(rows))
    except Exception:
        return pd.DataFrame(columns=["agent_code", "agent_name", "active"])


def agents_from_text(text: str) -> pd.DataFrame:
    rows = []
    pattern = re.compile(r"Оператор\s*-\s*([^(]+)\((\d{5,12})\)", re.IGNORECASE)
    for line in (text or "").splitlines():
        m = pattern.search(line.strip())
        if m:
            rows.append({"agent_code": m.group(2).strip(), "agent_name": m.group(1).strip(), "active": True})
    return normalize_agent_table(pd.DataFrame(rows)) if rows else pd.DataFrame(columns=["agent_code", "agent_name", "active"])


def merge_agents(base: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    base = normalize_agent_table(base)
    if new_rows is None or new_rows.empty:
        return base
    return normalize_agent_table(pd.concat([base, normalize_agent_table(new_rows)], ignore_index=True))


def apply_agent_names(df: pd.DataFrame, agent_table: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mapping = dict(zip(agent_table["agent_code"].astype(str), agent_table["agent_name"].astype(str)))
    out["agent_name"] = out["agent_code"].astype(str).map(mapping).fillna(out["agent_code"].astype(str))
    out.loc[out["agent_code"].astype(str).str.strip() == "", "agent_name"] = "Без оператора"
    return out


def shift_candidate_dates(df: pd.DataFrame, shift: ShiftSpec) -> List[date]:
    base_dates = set(df["call_date"].dropna().tolist())
    if not base_dates:
        return []
    sample = next(iter(base_dates))
    start_dt, end_dt = shift.bounds(sample)
    if end_dt.date() > start_dt.date():
        base_dates |= {d - timedelta(days=1) for d in base_dates}
    return sorted(base_dates)


def available_shift_dates(df: pd.DataFrame, shift: ShiftSpec) -> List[date]:
    dates = []
    for base_date in shift_candidate_dates(df, shift):
        start_dt, end_dt = shift.bounds(base_date)
        mask = (df["dt_start"] >= pd.Timestamp(start_dt)) & (df["dt_start"] < pd.Timestamp(end_dt))
        if mask.any():
            dates.append(base_date)
    return dates


def filter_by_shift(df: pd.DataFrame, shift: ShiftSpec, base_date: Optional[date]) -> Tuple[pd.DataFrame, Optional[Tuple[datetime, datetime]]]:
    if df.empty:
        return df, None
    dates = available_shift_dates(df, shift)
    if not dates:
        return df.iloc[0:0].copy(), None
    if base_date is None:
        parts = []
        for d in dates:
            start_dt, end_dt = shift.bounds(d)
            mask = (df["dt_start"] >= pd.Timestamp(start_dt)) & (df["dt_start"] < pd.Timestamp(end_dt))
            parts.append(df.loc[mask])
        return pd.concat(parts, ignore_index=True), (shift.bounds(dates[0])[0], shift.bounds(dates[-1])[1])
    start_dt, end_dt = shift.bounds(base_date)
    mask = (df["dt_start"] >= pd.Timestamp(start_dt)) & (df["dt_start"] < pd.Timestamp(end_dt))
    return df.loc[mask].copy().reset_index(drop=True), (start_dt, end_dt)


def detect_short_presence(df_shift: pd.DataFrame, min_slots: int) -> Tuple[Set[str], pd.DataFrame]:
    if df_shift.empty or min_slots <= 1:
        return set(), pd.DataFrame()
    data = df_shift[df_shift["agent_code"].astype(str).str.strip() != ""].copy()
    if data.empty:
        return set(), pd.DataFrame()
    grouped = data.groupby(["agent_code", "agent_name"], dropna=False).agg(
        **{
            "Активных получасов": ("slot", "nunique"),
            "Событий": ("disposition", "size"),
            "Первое событие": ("dt_start", "min"),
            "Последнее событие": ("dt_start", "max"),
        }
    ).reset_index()
    short = grouped[grouped["Активных получасов"] < int(min_slots)].copy()
    if not short.empty:
        short["Первое событие"] = short["Первое событие"].dt.strftime("%d.%m %H:%M")
        short["Последнее событие"] = short["Последнее событие"].dt.strftime("%d.%m %H:%M")
    return set(short["agent_code"].astype(str)) if not short.empty else set(), short


def slot_label(slot_value) -> str:
    start = pd.Timestamp(slot_value)
    return f"{start:%d.%m %H:%M}-{(start + pd.Timedelta(minutes=30)):%H:%M}"


def build_operator_title_part(metrics: Dict[str, pd.DataFrame], max_names: int = 8) -> str:
    profiles = metrics.get("profiles", pd.DataFrame())
    operators = metrics.get("operators", pd.DataFrame())
    if profiles is not None and not profiles.empty and "agent_name" in profiles.columns:
        names = profiles["agent_name"].dropna().astype(str).tolist()
    elif operators is not None and not operators.empty and "agent_name" in operators.columns:
        names = operators["agent_name"].dropna().astype(str).tolist()
    else:
        names = []
    names = [n.strip() for n in names if n and n.strip() and n.strip() != "Без оператора"]
    names = list(dict.fromkeys(names))
    if not names:
        return "без операторов"
    shown = names[:max_names]
    if len(names) > max_names:
        shown.append(f"и еще {len(names) - max_names}")
    return ", ".join(shown)


def build_period_prefix(window: Optional[Tuple[datetime, datetime]]) -> str:
    if not window:
        return "период не выбран"
    start_dt, end_dt = window
    if start_dt.date() == end_dt.date():
        return f"{start_dt:%d.%m.%Y}"
    if start_dt.year == end_dt.year:
        return f"{start_dt:%d.%m}-{end_dt:%d.%m.%Y}"
    return f"{start_dt:%d.%m.%Y}-{end_dt:%d.%m.%Y}"


def build_period_title(window: Optional[Tuple[datetime, datetime]], shift_name: str, operators_part: str) -> str:
    return f"{build_period_prefix(window)} ({operators_part})"


def build_report_base_name(window: Optional[Tuple[datetime, datetime]], operators_part: str) -> str:
    raw = build_period_title(window, "", operators_part)
    raw = re.sub(r'[\\/:*?"<>|]+', "-", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw[:160]


def build_profiles(df_operator: pd.DataFrame, window: Optional[Tuple[datetime, datetime]]) -> pd.DataFrame:
    if df_operator.empty:
        return pd.DataFrame()
    out = df_operator.groupby(["agent_code", "agent_name"], dropna=False).agg(
        Принято=("is_accepted", "sum"),
        Пропущено=("is_missed", "sum"),
        Всего=("disposition", "size"),
        **{
            "Первое событие": ("dt_start", "min"),
            "Последнее событие": ("dt_start", "max"),
            "Активных получасов": ("slot", "nunique"),
        },
    ).reset_index()
    denom = (out["Принято"] + out["Пропущено"]).replace(0, pd.NA)
    out["% пропущенных"] = (100.0 * out["Пропущено"] / denom).fillna(0).round(2)
    if window:
        total_slots = max(1, int((window[1] - window[0]).total_seconds() // 1800))
        out["Покрытие по событиям"] = (100.0 * out["Активных получасов"] / total_slots).round(1)
        out["Оценка окна"] = out["Покрытие по событиям"].apply(
            lambda x: "Короткое присутствие" if x < 20 else ("Неполное окно" if x < 55 else "Достаточное окно")
        )
    out["Первое событие"] = out["Первое событие"].dt.strftime("%d.%m %H:%M")
    out["Последнее событие"] = out["Последнее событие"].dt.strftime("%d.%m %H:%M")
    return out.sort_values(["Пропущено", "Принято"], ascending=[False, False])


def compute_metrics(df_shift: pd.DataFrame, watched_skills: List[str], only_watched: bool, conn_is_accepted: bool, skill_names: Dict[str, str], hidden_codes: Set[str], window: Optional[Tuple[datetime, datetime]]) -> Dict[str, pd.DataFrame]:
    df = df_shift.copy()
    accepted_set = set(DISP_ACCEPTED) | ({"CONN"} if conn_is_accepted else set())
    df["is_accepted"] = df["disposition"].isin(accepted_set)
    df["is_missed"] = df["disposition"].isin(DISP_MISSED)
    if only_watched and watched_skills:
        df["is_missed"] = df["is_missed"] & df["skill_code"].isin([str(x) for x in watched_skills])
    df["is_missed_no_agent"] = df["is_missed"] & (df["agent_code"].astype(str).str.strip() == "")
    df["skill_name"] = df["skill_code"].map(skill_names).fillna(df["skill_code"].replace("", "Без тематики"))

    accepted = int(df["is_accepted"].sum())
    missed = int(df["is_missed"].sum())
    missed_no_agent = int(df["is_missed_no_agent"].sum())
    denom = accepted + missed
    kpi = pd.DataFrame([{
        "Принято": accepted,
        "Пропущено": missed,
        "Пропущено без оператора": missed_no_agent,
        "% пропущенных": round(100.0 * missed / denom, 2) if denom else 0.0,
        "Всего событий": int(len(df)),
    }])

    ts = df.groupby("slot", dropna=False).agg(
        Принято=("is_accepted", "sum"),
        Пропущено=("is_missed", "sum"),
        Пропущено_без_оператора=("is_missed_no_agent", "sum"),
        Всего=("disposition", "size"),
    ).reset_index().sort_values("slot")

    op_source = df[(df["agent_code"].astype(str).str.strip() != "") & (~df["agent_code"].astype(str).isin(hidden_codes))].copy()
    if op_source.empty:
        operators = profiles = piv_acc = piv_mis = pd.DataFrame()
    else:
        operators = op_source.groupby("agent_name", dropna=False).agg(Принято=("is_accepted", "sum"), Пропущено=("is_missed", "sum"), Всего=("disposition", "size")).reset_index()
        denom_ops = (operators["Принято"] + operators["Пропущено"]).replace(0, pd.NA)
        operators["% пропущенных"] = (100.0 * operators["Пропущено"] / denom_ops).fillna(0).round(2)
        operators = operators.sort_values(["Пропущено", "Принято"], ascending=[False, False])
        profiles = build_profiles(op_source, window)
        piv_acc = op_source[op_source["is_accepted"]].pivot_table(index="slot", columns="agent_name", values="is_accepted", aggfunc="sum", fill_value=0).sort_index()
        piv_mis = op_source[op_source["is_missed"]].pivot_table(index="slot", columns="agent_name", values="is_missed", aggfunc="sum", fill_value=0).sort_index()

    skills = df.groupby("skill_name", dropna=False).agg(Принято=("is_accepted", "sum"), Пропущено=("is_missed", "sum"), Всего=("disposition", "size")).reset_index().sort_values(["Пропущено", "Принято"], ascending=[False, False])

    anomalies = build_anomalies(ts)
    peaks = build_peaks(ts)

    return {"kpi": kpi, "timeseries": ts, "operators": operators, "profiles": profiles, "pivot_accepted": piv_acc, "pivot_missed": piv_mis, "skills": skills, "anomalies": anomalies, "peaks": peaks, "events": df.sort_values("dt_start").reset_index(drop=True)}


def build_anomalies(ts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in ts.iterrows():
        accepted = int(row["Принято"])
        missed = int(row["Пропущено"])
        no_agent = int(row["Пропущено_без_оператора"])
        total = int(row["Всего"])
        if missed > 0 and accepted == 0:
            rows.append({"Интервал": slot_label(row["slot"]), "Тип": "Нет принятых при пропусках", "Принято": accepted, "Пропущено": missed, "Без оператора": no_agent, "Всего": total, "Комментарий": "Проверить доступность операторов и маршрутизацию"})
        elif no_agent > 0:
            rows.append({"Интервал": slot_label(row["slot"]), "Тип": "Пропущено без оператора", "Принято": accepted, "Пропущено": missed, "Без оператора": no_agent, "Всего": total, "Комментарий": "Проверить очередь, skill и доступность операторов"})
    return pd.DataFrame(rows)


def build_peaks(ts: pd.DataFrame) -> pd.DataFrame:
    if ts.empty:
        return pd.DataFrame()
    out = ts.copy()
    denom = (out["Принято"] + out["Пропущено"]).replace(0, pd.NA)
    out["% пропущенных"] = (100.0 * out["Пропущено"] / denom).fillna(0).round(2)
    out["Интервал"] = out["slot"].apply(slot_label)
    return out.sort_values(["Всего", "Пропущено"], ascending=[False, False])[["Интервал", "Всего", "Принято", "Пропущено", "% пропущенных"]]


def assess_risk(kpi: Dict[str, float], anomalies: pd.DataFrame, yellow: float, red: float) -> Tuple[str, List[str]]:
    pct = float(kpi.get("% пропущенных", 0))
    missed = int(kpi.get("Пропущено", 0))
    no_agent = int(kpi.get("Пропущено без оператора", 0))
    anomaly_count = 0 if anomalies is None or anomalies.empty else len(anomalies)
    level = "Зеленый"
    reasons = []
    if missed == 0:
        reasons.append("Пропущенных вызовов нет.")
    if pct >= red:
        level = "Красный"
        reasons.append(f"Процент пропущенных {pct}% выше красного порога {red}%.")
    elif pct >= yellow:
        level = "Желтый"
        reasons.append(f"Процент пропущенных {pct}% выше желтого порога {yellow}%.")
    if anomaly_count >= 3:
        level = "Красный"
        reasons.append(f"Найдено {anomaly_count} аномальных интервалов.")
    elif anomaly_count > 0 and level == "Зеленый":
        level = "Желтый"
        reasons.append(f"Есть аномальные интервалы: {anomaly_count}.")
    if no_agent >= 5:
        level = "Красный"
        reasons.append(f"Много пропущенных без оператора: {no_agent}.")
    elif no_agent > 0 and level == "Зеленый":
        level = "Желтый"
        reasons.append(f"Есть пропущенные без оператора: {no_agent}.")
    if not reasons:
        reasons.append("Критичных отклонений по заданным правилам не найдено.")
    return level, reasons


def build_summary(metrics: Dict[str, pd.DataFrame], risk: str, reasons: List[str], window: Optional[Tuple[datetime, datetime]], hidden_presence: pd.DataFrame, inactive_codes: Set[str]) -> List[str]:
    kpi = metrics["kpi"].iloc[0].to_dict()
    lines = []
    if window:
        lines.append(f"Окно анализа: {window[0]:%d.%m.%Y %H:%M} - {window[1]:%d.%m.%Y %H:%M}.")
    lines.append(f"Итог: принято {int(kpi['Принято'])}, пропущено {int(kpi['Пропущено'])}, без оператора {int(kpi['Пропущено без оператора'])}, процент пропущенных {kpi['% пропущенных']}%.")
    lines.append(f"Риск-статус: {risk}. " + " ".join(reasons))
    if not metrics["peaks"].empty:
        top = metrics["peaks"].iloc[0]
        lines.append(f"Пиковая нагрузка: {top['Интервал']}, всего событий {int(top['Всего'])}, пропущено {int(top['Пропущено'])}.")
    if not metrics["anomalies"].empty:
        lines.append("Проблемные интервалы: " + ", ".join(metrics["anomalies"]["Интервал"].head(3).astype(str).tolist()) + ".")
    if inactive_codes:
        lines.append("Из операторских таблиц исключены неактивные коды: " + ", ".join(sorted(inactive_codes)) + ".")
    if hidden_presence is not None and not hidden_presence.empty:
        lines.append("Короткие входы не включены в профили: " + ", ".join(hidden_presence["agent_code"].astype(str).head(10).tolist()) + ".")
    return lines


def show_table(df: pd.DataFrame, rows: int = MAX_TABLE_ROWS):
    if df is None or df.empty:
        st.caption("Нет данных")
        return
    st.dataframe(df.head(rows), use_container_width=True)
    if len(df) > rows:
        st.caption(f"Показаны первые {rows} строк из {len(df)}. Полные данные доступны в выгрузке.")


def safe_reindex(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    return df.reindex(columns=cols).fillna(0).astype(int)


def color_missed_matrix(df: pd.DataFrame):
    def color_value(value):
        try:
            value = int(value)
        except Exception:
            return ""
        if value >= 3:
            return "background-color: #f8d7da; font-weight: 700;"
        if value >= 1:
            return "background-color: #fff3cd;"
        return ""
    try:
        return df.style.map(color_value)
    except Exception:
        try:
            return df.style.applymap(color_value)
        except Exception:
            return df


@st.cache_data(show_spinner=False, max_entries=3)
def make_excel_report_cached(metrics: Dict[str, pd.DataFrame], summary_lines: List[str], risk: str, period_title: str, include_events: bool) -> bytes:
    output = io.BytesIO()
    summary_df = pd.DataFrame({"Показатель": ["Аналитический период", "Риск-статус"] + [f"Вывод {i + 1}" for i in range(len(summary_lines))], "Значение": [period_title, risk] + summary_lines})
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Итог")
        sheet_map = {"timeseries": "Динамика", "operators": "Операторы", "profiles": "Профили", "skills": "Тематики", "anomalies": "Аномалии", "peaks": "Пики"}
        for key, sheet in sheet_map.items():
            df = metrics.get(key, pd.DataFrame())
            (df if df is not None else pd.DataFrame()).to_excel(writer, index=False, sheet_name=sheet)
        if include_events:
            metrics["events"].head(MAX_EXPORT_EVENTS_IN_XLSX).to_excel(writer, index=False, sheet_name="События")
    return output.getvalue()


@st.cache_data(show_spinner=False, max_entries=3)
def make_word_report_cached(metrics: Dict[str, pd.DataFrame], summary_lines: List[str], risk: str, period_title: str) -> bytes:
    def to_html(df: pd.DataFrame, limit: int = 200) -> str:
        if df is None or df.empty:
            return "<p>Нет данных</p>"
        return df.head(limit).to_html(index=False, border=1)
    html = f"""
    <html><head><meta charset="utf-8"><style>
    body {{ font-family: Arial, sans-serif; font-size: 12pt; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 18px; }}
    th, td {{ border: 1px solid #999; padding: 6px; }} th {{ background: #f2f2f2; }}
    </style></head><body>
    <h1>Анализ смены Avaya CMS</h1><h2>{period_title}</h2><h2>Риск-статус: {risk}</h2>
    <h2>Автоматический вывод</h2><ul>{''.join(f'<li>{line}</li>' for line in summary_lines)}</ul>
    <h2>KPI</h2>{to_html(metrics.get('kpi'))}
    <h2>Аномалии</h2>{to_html(metrics.get('anomalies'))}
    <h2>Операторы</h2>{to_html(metrics.get('operators'))}
    <h2>Профили операторов</h2>{to_html(metrics.get('profiles'))}
    <h2>Тематики</h2>{to_html(metrics.get('skills'))}
    <h2>Пики нагрузки</h2>{to_html(metrics.get('peaks'))}
    </body></html>
    """
    return html.encode("utf-8")


@st.cache_data(show_spinner=False, max_entries=3)
def make_csv_cached(events: pd.DataFrame) -> bytes:
    return events.to_csv(index=False).encode("utf-8-sig")


# Session state
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0
if "exports_ready" not in st.session_state:
    st.session_state.exports_ready = False
if "agent_table" not in st.session_state or "skill_table" not in st.session_state:
    agents, skills = load_settings()
    st.session_state.agent_table = agents
    st.session_state.skill_table = skills

st.title("Avaya CMS — анализ смен")

with st.sidebar:
    st.header("Файл")
    if st.button("Очистить отчет и загрузить новый", use_container_width=True):
        st.session_state.uploader_key += 1
        st.session_state.exports_ready = False
        st.cache_data.clear()
        st.rerun()

    uploaded = st.file_uploader("HTML отчет Avaya", type=["html", "htm"], key=f"html_{st.session_state.uploader_key}")

    st.divider()
    st.header("Смена")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Ночь", use_container_width=True):
            st.session_state.mode = "night"
    with c2:
        if st.button("День", use_container_width=True):
            st.session_state.mode = "day"
    if "mode" not in st.session_state:
        st.session_state.mode = "night"
    mode = st.radio("Режим", ["night", "day", "custom"], format_func=lambda x: SHIFT_PRESETS[x].name, index=["night", "day", "custom"].index(st.session_state.mode))
    st.session_state.mode = mode
    if mode == "custom":
        sh = st.number_input("Старт: час", 0, 23, 18)
        sm = st.number_input("Старт: мин", 0, 59, 30)
        eh = st.number_input("Конец: час", 0, 23, 6)
        em = st.number_input("Конец: мин", 0, 59, 30)
        SHIFT_PRESETS["custom"] = ShiftSpec("Кастом", (int(sh), int(sm)), (int(eh), int(em)), None)
    shift = SHIFT_PRESETS[mode]

    st.divider()
    st.header("Правила")
    conn_is_accepted = st.checkbox("Считать CONN как принято", value=False)
    only_watched = st.checkbox("Пропущенные считать только по выбранным skills", value=(mode == "night"))
    skill_table = normalize_skill_table(st.session_state.skill_table)
    skill_options = skill_table["skill_code"].astype(str).tolist()
    watched_default = skill_table.loc[skill_table["watched"], "skill_code"].astype(str).tolist()
    watched_skills = st.multiselect("Skills", options=skill_options, default=watched_default)
    yellow = st.number_input("Желтый порог %", 0.0, 100.0, 5.0, 0.5)
    red = st.number_input("Красный порог %", 0.0, 100.0, 10.0, 0.5)

    st.divider()
    st.header("Стабильность")
    safe_ui = st.checkbox("Безопасный режим интерфейса", value=True)
    show_matrix = st.checkbox("Показывать матрицу", value=not safe_ui)
    show_chart = st.checkbox("Показывать график", value=not safe_ui)
    include_events_in_excel = st.checkbox("Включать события в Excel", value=False)

    st.divider()
    st.header("Человеческий фактор")
    exclude_inactive = st.checkbox("Скрывать неактивных операторов", value=True)
    guard_short = st.checkbox("Не считать короткий вход полноценной сменой", value=True)
    min_slots = st.number_input("Минимум активных получасов", 1, 24, 2, 1)

    st.divider()
    st.header("Наглядность")
    top_n = st.slider("Топ N", 3, 30, 10)
    max_ops = st.slider("Макс операторов в матрице", 3, 60, 12)

    st.divider()
    show_settings = st.checkbox("Редактор справочников", value=False)
    if show_settings:
        st.subheader("Операторы")
        map_file = st.file_uploader("CSV/XLSX операторов", type=["csv", "txt", "xlsx", "xls"], key="mapping_upload")
        pasted = st.text_area("Оператор - Фамилия ИО (код)", height=100)
        a1, a2 = st.columns(2)
        with a1:
            if st.button("Добавить", use_container_width=True):
                st.session_state.agent_table = merge_agents(merge_agents(st.session_state.agent_table, load_agent_file(map_file)), agents_from_text(pasted))
        with a2:
            if st.button("Сброс", use_container_width=True):
                st.session_state.agent_table = default_agent_table()
        st.session_state.agent_table = normalize_agent_table(st.data_editor(normalize_agent_table(st.session_state.agent_table), use_container_width=True, num_rows="dynamic"))
        st.subheader("Skills")
        st.session_state.skill_table = normalize_skill_table(st.data_editor(normalize_skill_table(st.session_state.skill_table), use_container_width=True, num_rows="dynamic"))
        if st.button("Сохранить настройки", use_container_width=True):
            ok, msg = save_settings(st.session_state.agent_table, st.session_state.skill_table)
            st.success(msg) if ok else st.error(msg)

if uploaded is None:
    st.info("Загрузи HTML отчет Avaya слева. Если после скачивания отчетов интерфейс зависает, используй кнопку очистки перед новым файлом.")
    st.stop()

try:
    with st.spinner("Читаю HTML отчет Avaya..."):
        raw_bytes = uploaded.getvalue()
        df_raw = parse_avaya_html(raw_bytes)
        df, found_cols, problems = normalize_calls(df_raw)
except Exception as exc:
    st.error("Не удалось обработать файл. Попробуй очистить отчет и загрузить HTML повторно.")
    st.caption(str(exc))
    st.stop()

if problems:
    st.error("Файл не распознан: " + ", ".join(problems))
    if found_cols:
        st.dataframe(pd.DataFrame([{"Поле": k, "Колонка": v or "Не найдена"} for k, v in found_cols.items()]), use_container_width=True)
    st.stop()

agent_table = normalize_agent_table(st.session_state.agent_table)
skill_table = normalize_skill_table(st.session_state.skill_table)
df = apply_agent_names(df, agent_table)

dates = available_shift_dates(df, shift)
if not dates:
    st.warning("В выбранном режиме смены нет событий.")
    st.stop()
if len(dates) == 1:
    base_date = dates[0]
    st.caption(f"В файле найдена 1 смена: {base_date:%d.%m.%Y}")
else:
    options = ["Все смены в файле"] + [d.strftime("%d.%m.%Y") for d in dates]
    chosen = st.selectbox("Дата смены", options=options, index=0)
    base_date = None if chosen == "Все смены в файле" else datetime.strptime(chosen, "%d.%m.%Y").date()

df_shift, window = filter_by_shift(df, shift, base_date)
if df_shift.empty:
    st.warning("В выбранном окне смены нет событий.")
    st.stop()

inactive_codes = set()
hidden_codes = set()
if exclude_inactive:
    inactive_codes = set(agent_table.loc[~agent_table["active"], "agent_code"].astype(str))
    hidden_codes |= inactive_codes
hidden_presence = pd.DataFrame()
if guard_short:
    short_codes, hidden_presence = detect_short_presence(df_shift, int(min_slots))
    hidden_codes |= short_codes

skill_names = dict(zip(skill_table["skill_code"].astype(str), skill_table["skill_name"].astype(str)))
metrics = compute_metrics(df_shift, [str(x) for x in watched_skills], only_watched, conn_is_accepted, skill_names, hidden_codes, window)
kpi = metrics["kpi"].iloc[0].to_dict()
risk, reasons = assess_risk(kpi, metrics["anomalies"], float(yellow), float(red))
summary = build_summary(metrics, risk, reasons, window, hidden_presence, inactive_codes)
operators_part = build_operator_title_part(metrics)
period_title = build_period_title(window, shift.name, operators_part)
report_base_name = build_report_base_name(window, operators_part)

if window:
    st.subheader(f"Окно анализа: {window[0]:%d.%m.%Y %H:%M} - {window[1]:%d.%m.%Y %H:%M} ({shift.name})")
else:
    st.subheader(f"Окно анализа: {shift.name}")
st.caption(f"Название аналитического периода для отчетов: {period_title}")

with st.expander("Проверка входного файла", expanded=False):
    st.dataframe(pd.DataFrame([{"Поле": k, "Колонка": v or "Не найдена"} for k, v in found_cols.items()]), use_container_width=True)
    st.caption(f"Размер файла: {len(raw_bytes) / 1024 / 1024:.2f} МБ. Строк в HTML: {len(df_raw)}. Распознано событий: {len(df)}.")

st.markdown("### Автоматический вывод")
if risk == "Красный":
    st.error(f"Риск-статус: {risk}")
elif risk == "Желтый":
    st.warning(f"Риск-статус: {risk}")
else:
    st.success(f"Риск-статус: {risk}")
for line in summary:
    st.write("- " + line)

st.markdown("### KPI")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Принято", int(kpi["Принято"]))
c2.metric("Пропущено", int(kpi["Пропущено"]))
c3.metric("Без оператора", int(kpi["Пропущено без оператора"]))
c4.metric("% пропущенных", f"{kpi['% пропущенных']}%")
c5.metric("Всего", int(kpi["Всего событий"]))

if show_chart and not metrics["timeseries"].empty:
    st.markdown("### Динамика")
    st.line_chart(metrics["timeseries"].set_index("slot")[["Принято", "Пропущено", "Пропущено_без_оператора"]])

st.markdown("### Пики нагрузки")
show_table(metrics["peaks"].head(top_n), rows=top_n)

st.markdown("### Аномалии")
show_table(metrics["anomalies"], rows=MAX_TABLE_ROWS)

st.markdown("### Операторы и тематики")
left, right = st.columns(2)
with left:
    st.markdown(f"**Топ-{top_n} операторов по пропущенным**")
    show_table(metrics["operators"].head(top_n), rows=top_n)
with right:
    st.markdown(f"**Топ-{top_n} тематик по пропущенным**")
    show_table(metrics["skills"].head(top_n), rows=top_n)

with st.expander("Профиль операторов", expanded=not safe_ui):
    st.caption("Профиль строится по фактическим событиям оператора, а не по предположению, что он был в системе всю смену.")
    show_table(metrics["profiles"], rows=MAX_TABLE_ROWS)
    if hidden_presence is not None and not hidden_presence.empty:
        st.markdown("**Короткие входы, исключенные из профилей**")
        show_table(hidden_presence, rows=MAX_TABLE_ROWS)

if show_matrix:
    st.markdown("### Матрица по получасам")
    piv_acc = metrics["pivot_accepted"]
    piv_mis = metrics["pivot_missed"]
    all_ops = sorted(set((list(piv_acc.columns) if not piv_acc.empty else []) + (list(piv_mis.columns) if not piv_mis.empty else []) + (metrics["operators"]["agent_name"].tolist() if not metrics["operators"].empty else [])))
    default_ops = (metrics["operators"]["agent_name"].tolist() if not metrics["operators"].empty else [])[:max_ops]
    selected = st.multiselect("Операторы для матрицы", options=all_ops, default=[x for x in default_ops if x in all_ops])
    if selected:
        acc_show = safe_reindex(piv_acc, selected)
        mis_show = safe_reindex(piv_mis, selected)
        m1, m2 = st.columns(2)
        with m1:
            st.markdown("**Принятые**")
            show_table(acc_show, rows=MAX_TABLE_ROWS)
        with m2:
            st.markdown("**Пропущенные**")
            cell_count = max(1, mis_show.shape[0] * mis_show.shape[1])
            if cell_count <= 1200:
                st.dataframe(color_missed_matrix(mis_show), use_container_width=True)
            else:
                st.caption("Матрица большая, подсветка отключена для стабильности интерфейса.")
                show_table(mis_show, rows=MAX_TABLE_ROWS)
else:
    st.caption("Матрица скрыта безопасным режимом. Ее можно включить в боковой панели.")

st.markdown("### Экспорт")
st.caption("Файлы формируются только по кнопке. Название начинается с аналитического периода и ФИО операторов.")
if st.button("Подготовить файлы для скачивания", use_container_width=True):
    st.session_state.exports_ready = True

if st.session_state.exports_ready:
    with st.spinner("Готовлю файлы..."):
        excel_bytes = make_excel_report_cached(metrics, summary, risk, period_title, include_events_in_excel)
        word_bytes = make_word_report_cached(metrics, summary, risk, period_title)
        csv_bytes = make_csv_cached(metrics["events"])
    e1, e2, e3 = st.columns(3)
    with e1:
        safe_download_button("Скачать Excel", data=excel_bytes, file_name=f"{report_base_name} avaya_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    with e2:
        safe_download_button("Скачать Word", data=word_bytes, file_name=f"{report_base_name} avaya_report.doc", mime="application/msword", use_container_width=True)
    with e3:
        safe_download_button("Скачать CSV событий", data=csv_bytes, file_name=f"{report_base_name} avaya_events.csv", mime="text/csv", use_container_width=True)

with st.expander("Сырые события", expanded=False):
    show_table(metrics["events"], rows=500)
