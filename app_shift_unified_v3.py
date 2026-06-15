# app_shift_unified_v3.py
# Streamlit: анализатор Avaya CMS Call Records для смен КЦ.
#
# Запуск:
#   pip install streamlit pandas beautifulsoup4 openpyxl lxml
#   streamlit run app_shift_unified_v3.py
#
# Что умеет:
# - анализ ночной, дневной и кастомной смены;
# - загрузка HTML из Avaya CMS;
# - справочник операторов и skills с сохранением в локальный JSON;
# - исключение неактивных операторов и коротких человеческих входов из операторских профилей;
# - автоматический вывод по смене, риск-статус, аномалии, пики нагрузки;
# - профиль операторов, цветовая матрица, экспорт в Excel и Word-совместимый отчет.

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


SETTINGS_FILE = Path("avaya_shift_settings.json")

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

NIGHT_START = (18, 30)
NIGHT_END = (6, 30)
DAY_START = (6, 30)
DAY_END = (18, 30)

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
    "night": ShiftSpec("Ночная (18:30-06:30)", NIGHT_START, NIGHT_END, None),
    "day": ShiftSpec("Дневная (06:30-18:30)", DAY_START, DAY_END, False),
    "custom": ShiftSpec("Кастом", NIGHT_START, NIGHT_END, None),
}


def _safe_decode(data: bytes) -> str:
    for enc in ("cp1251", "windows-1251", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _clean_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\xa0", " ").strip()
    return "" if s in {"&nbsp;", "\u00a0", "nan", "None"} else s


def _first_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    low_map = {str(c).lower().strip(): c for c in df.columns}
    for c in candidates:
        hit = low_map.get(c.lower().strip())
        if hit is not None:
            return hit
    return None


def _find_target_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None

    def score(tbl) -> int:
        headers = [_clean_text(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
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


@st.cache_data(show_spinner=False)
def parse_avaya_html(html_bytes: bytes) -> pd.DataFrame:
    try:
        text = _safe_decode(html_bytes)
        soup = BeautifulSoup(text, "lxml")
        table = _find_target_table(soup)
        if table is None:
            return pd.DataFrame()

        headers = [_clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]
        if not headers:
            return pd.DataFrame()

        rows: List[List[str]] = []
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if not cells:
                continue
            row = [_clean_text(td.get_text(" ", strip=True)) for td in cells]
            if len(row) < len(headers):
                row += [""] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            rows.append(row)

        return pd.DataFrame(rows, columns=headers)
    except Exception:
        return pd.DataFrame()


def detect_avaya_columns(df_raw: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {
        "Дата": _first_existing_col(df_raw, ["Дата", "Date"]),
        "Время нач.": _first_existing_col(df_raw, ["Время нач.", "Время нач", "Time"]),
        "Размещение": _first_existing_col(df_raw, ["Размещение", "Disposition"]),
        "Split/Skill": _first_existing_col(df_raw, ["Split/Skill", "Skill", "Split"]),
        "Оператор": _first_existing_col(df_raw, ["Имена пользователей", "Agent", "Пользователь", "User"]),
    }


def missing_required_columns(found: Dict[str, Optional[str]]) -> List[str]:
    return [name for name in ("Дата", "Время нач.", "Размещение") if not found.get(name)]


def normalize_calls(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()

    found = detect_avaya_columns(df_raw)
    if missing_required_columns(found):
        return pd.DataFrame()

    df = df_raw.copy()
    col_date = found["Дата"]
    col_time = found["Время нач."]
    col_disp = found["Размещение"]
    col_skill = found["Split/Skill"]
    col_agent = found["Оператор"]

    dt_str = (df[col_date].astype(str).str.strip() + " " + df[col_time].astype(str).str.strip()).str.strip()
    df["dt_start"] = pd.to_datetime(dt_str, format="%d.%m.%Y %H:%M:%S", errors="coerce")
    df = df.dropna(subset=["dt_start"]).reset_index(drop=True)

    df["disposition"] = df[col_disp].astype(str).str.strip().str.upper()
    df["skill_raw"] = "" if col_skill is None else df[col_skill].astype(str).str.strip()
    df["agent_code"] = "" if col_agent is None else df[col_agent].astype(str).str.strip()
    df["agent_code"] = df["agent_code"].replace({"nan": "", "None": ""})

    df["skill_code"] = df["skill_raw"].str.extract(r"(\d+)", expand=False).fillna("").astype(str)
    df["call_date"] = df["dt_start"].dt.date
    df["slot"] = df["dt_start"].apply(
        lambda x: pd.Timestamp(x).replace(minute=(0 if x.minute < 30 else 30), second=0, microsecond=0)
    )

    return df


def _bool_value(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return True
    return str(value).strip().lower() not in {"0", "false", "нет", "no", "inactive", "неактивен"}


def default_agent_table() -> pd.DataFrame:
    return pd.DataFrame(
        [{"agent_code": code, "agent_name": name, "active": True} for code, name in sorted(DEFAULT_AGENT_MAP.items())]
    )


def default_skill_table() -> pd.DataFrame:
    return pd.DataFrame(
        [{"skill_code": code, "skill_name": name, "watched": True} for code, name in sorted(DEFAULT_SKILL_NAMES.items())]
    )


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
    out["active"] = out["active"].apply(_bool_value)
    out = out[(out["agent_code"] != "") & (out["agent_name"] != "")]
    out = out.drop_duplicates(subset=["agent_code"], keep="last").sort_values("agent_code").reset_index(drop=True)
    return out


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
    out["watched"] = out["watched"].apply(_bool_value)
    out = out[out["skill_code"] != ""]
    out.loc[out["skill_name"] == "", "skill_name"] = out["skill_code"]
    out = out.drop_duplicates(subset=["skill_code"], keep="last").sort_values("skill_code").reset_index(drop=True)
    return out


def load_settings() -> Dict[str, pd.DataFrame]:
    if not SETTINGS_FILE.exists():
        return {"agents": default_agent_table(), "skills": default_skill_table()}
    try:
        payload = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        return {
            "agents": normalize_agent_table(pd.DataFrame(payload.get("agents", []))),
            "skills": normalize_skill_table(pd.DataFrame(payload.get("skills", []))),
        }
    except Exception:
        return {"agents": default_agent_table(), "skills": default_skill_table()}


def save_settings(agent_table: pd.DataFrame, skill_table: pd.DataFrame) -> Tuple[bool, str]:
    try:
        payload = {
            "agents": normalize_agent_table(agent_table).to_dict(orient="records"),
            "skills": normalize_skill_table(skill_table).to_dict(orient="records"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, f"Настройки сохранены в {SETTINGS_FILE}"
    except Exception as exc:
        return False, f"Не удалось сохранить настройки: {exc}"


def agent_map_from_table(agent_table: pd.DataFrame) -> Dict[str, str]:
    table = normalize_agent_table(agent_table)
    return dict(zip(table["agent_code"].astype(str), table["agent_name"].astype(str)))


def active_agent_codes(agent_table: pd.DataFrame) -> Set[str]:
    table = normalize_agent_table(agent_table)
    return set(table.loc[table["active"], "agent_code"].astype(str))


def inactive_agent_codes(agent_table: pd.DataFrame) -> Set[str]:
    table = normalize_agent_table(agent_table)
    return set(table.loc[~table["active"], "agent_code"].astype(str))


def skill_name_map_from_table(skill_table: pd.DataFrame) -> Dict[str, str]:
    table = normalize_skill_table(skill_table)
    return dict(zip(table["skill_code"].astype(str), table["skill_name"].astype(str)))


def watched_skills_from_table(skill_table: pd.DataFrame) -> List[str]:
    table = normalize_skill_table(skill_table)
    return table.loc[table["watched"], "skill_code"].astype(str).tolist()


def extract_agents_from_registry_text(text: str) -> pd.DataFrame:
    if not text:
        return pd.DataFrame(columns=["agent_code", "agent_name", "active"])

    rows = []
    pattern = re.compile(r"Оператор\s*-\s*([^(]+)\((\d{5,12})\)", re.IGNORECASE)
    for line in text.splitlines():
        match = pattern.search(line.strip())
        if match:
            rows.append(
                {
                    "agent_code": match.group(2).strip(),
                    "agent_name": match.group(1).strip(),
                    "active": True,
                }
            )
    return normalize_agent_table(pd.DataFrame(rows))


def load_mapping_file(uploaded) -> pd.DataFrame:
    if uploaded is None:
        return pd.DataFrame(columns=["agent_code", "agent_name", "active"])

    try:
        name = getattr(uploaded, "name", "").lower()
        data = uploaded.getvalue()

        if name.endswith((".csv", ".txt")):
            df = None
            for sep in (";", ",", "\t"):
                try:
                    candidate = pd.read_csv(io.BytesIO(data), sep=sep, dtype=str)
                    if candidate.shape[1] >= 2:
                        df = candidate
                        break
                except Exception:
                    continue
            if df is None:
                return pd.DataFrame(columns=["agent_code", "agent_name", "active"])
        else:
            df = pd.read_excel(io.BytesIO(data), dtype=str)

        df = df.fillna("")
        cols = [str(c).lower().strip() for c in df.columns]
        code_col = None
        name_col = None
        active_col = None

        for normalized, raw in zip(cols, df.columns):
            if code_col is None and any(k in normalized for k in ("agent", "code", "номер", "id", "код")):
                code_col = raw
            if name_col is None and any(k in normalized for k in ("name", "имя", "фио", "оператор")):
                name_col = raw
            if active_col is None and any(k in normalized for k in ("active", "актив", "статус")):
                active_col = raw

        if code_col is None or name_col is None:
            code_col, name_col = df.columns[0], df.columns[1]

        rows = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            name = str(row[name_col]).strip()
            if not code or not name:
                continue
            rows.append(
                {
                    "agent_code": code,
                    "agent_name": name,
                    "active": _bool_value(row[active_col]) if active_col is not None else True,
                }
            )

        return normalize_agent_table(pd.DataFrame(rows))
    except Exception:
        return pd.DataFrame(columns=["agent_code", "agent_name", "active"])


def merge_agent_tables(base: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    base = normalize_agent_table(base)
    new_rows = normalize_agent_table(new_rows)
    if new_rows.empty:
        return base
    return normalize_agent_table(pd.concat([base, new_rows], ignore_index=True))


def apply_agent_map(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    out["agent_name"] = out["agent_code"].astype(str).map(mapping).fillna(out["agent_code"].astype(str))
    out.loc[out["agent_code"].astype(str).str.strip() == "", "agent_name"] = "Без оператора"
    return out


def shift_candidate_dates(df: pd.DataFrame, shift: ShiftSpec) -> List[date]:
    base_dates = set(df["call_date"].dropna().tolist())
    crosses = shift.bounds(next(iter(base_dates)))[1].date() > shift.bounds(next(iter(base_dates)))[0].date() if base_dates else False
    if crosses:
        base_dates |= {d - timedelta(days=1) for d in base_dates}
    return sorted(base_dates)


def available_shift_dates(df: pd.DataFrame, shift: ShiftSpec) -> List[date]:
    if df.empty:
        return []
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

    if base_date is None:
        dates = available_shift_dates(df, shift)
        if not dates:
            return df.iloc[0:0].copy(), None

        parts = []
        for current_date in dates:
            start_dt, end_dt = shift.bounds(current_date)
            mask = (df["dt_start"] >= pd.Timestamp(start_dt)) & (df["dt_start"] < pd.Timestamp(end_dt))
            parts.append(df.loc[mask])

        out = pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()
        return out, (shift.bounds(dates[0])[0], shift.bounds(dates[-1])[1])

    start_dt, end_dt = shift.bounds(base_date)
    mask = (df["dt_start"] >= pd.Timestamp(start_dt)) & (df["dt_start"] < pd.Timestamp(end_dt))
    return df.loc[mask].copy().reset_index(drop=True), (start_dt, end_dt)


def detect_short_presence_agents(df_shift: pd.DataFrame, min_active_slots: int) -> Tuple[Set[str], pd.DataFrame]:
    if df_shift.empty or min_active_slots <= 1:
        return set(), pd.DataFrame(columns=["agent_code", "agent_name", "Активных получасов", "Событий"])

    df_agents = df_shift[df_shift["agent_code"].astype(str).str.strip() != ""].copy()
    if df_agents.empty:
        return set(), pd.DataFrame(columns=["agent_code", "agent_name", "Активных получасов", "Событий"])

    grouped = (
        df_agents.groupby(["agent_code", "agent_name"], dropna=False)
        .agg(
            **{
                "Активных получасов": ("slot", "nunique"),
                "Событий": ("disposition", "size"),
                "Первое событие": ("dt_start", "min"),
                "Последнее событие": ("dt_start", "max"),
            }
        )
        .reset_index()
    )
    short = grouped[grouped["Активных получасов"] < int(min_active_slots)].copy()
    return set(short["agent_code"].astype(str)), short


def slot_interval_label(slot_value) -> str:
    if pd.isna(slot_value):
        return ""
    start = pd.Timestamp(slot_value)
    end = start + pd.Timedelta(minutes=30)
    return f"{start:%d.%m %H:%M}-{end:%H:%M}"


def compute_metrics(
    df_shift: pd.DataFrame,
    watched_skills: List[str],
    only_watched_for_missed: bool,
    treat_conn_as_accepted: bool,
    skill_names: Dict[str, str],
    hidden_agent_codes: Set[str],
    window: Optional[Tuple[datetime, datetime]],
) -> Dict[str, pd.DataFrame]:
    empty = pd.DataFrame()
    if df_shift.empty:
        return {
            "kpi": pd.DataFrame([{"Принято": 0, "Пропущено": 0, "Пропущено без оператора": 0, "% пропущенных": 0.0, "Всего событий": 0}]),
            "timeseries": empty,
            "operators": empty,
            "operator_profiles": empty,
            "pivot_accepted": empty,
            "pivot_missed": empty,
            "skills": empty,
            "anomalies": empty,
            "peaks": empty,
            "events": df_shift,
        }

    df = df_shift.copy()
    accepted_set = set(DISP_ACCEPTED) | ({"CONN"} if treat_conn_as_accepted else set())

    df["is_accepted"] = df["disposition"].isin(accepted_set)
    df["is_missed"] = df["disposition"].isin(DISP_MISSED)
    if only_watched_for_missed and watched_skills:
        df["is_missed"] = df["is_missed"] & df["skill_code"].isin([str(x) for x in watched_skills])

    df["is_missed_no_agent"] = df["is_missed"] & (df["agent_code"].astype(str).str.strip() == "")
    df["skill_name"] = df["skill_code"].map(skill_names).fillna(df["skill_code"].replace("", "Без тематики"))

    accepted = int(df["is_accepted"].sum())
    missed = int(df["is_missed"].sum())
    missed_no_agent = int(df["is_missed_no_agent"].sum())
    denom = accepted + missed

    kpi = pd.DataFrame(
        [
            {
                "Принято": accepted,
                "Пропущено": missed,
                "Пропущено без оператора": missed_no_agent,
                "% пропущенных": round(100.0 * missed / denom, 2) if denom else 0.0,
                "Всего событий": int(len(df)),
            }
        ]
    )

    ts = (
        df.groupby("slot", dropna=False)
        .agg(
            Принято=("is_accepted", "sum"),
            Пропущено=("is_missed", "sum"),
            Пропущено_без_оператора=("is_missed_no_agent", "sum"),
            Всего=("disposition", "size"),
        )
        .reset_index()
        .sort_values("slot")
    )

    df_with_agent = df[df["agent_code"].astype(str).str.strip() != ""].copy()
    df_operator = df_with_agent[~df_with_agent["agent_code"].astype(str).isin(hidden_agent_codes)].copy()

    if df_operator.empty:
        ops = pd.DataFrame(columns=["agent_name", "Принято", "Пропущено", "% пропущенных"])
        piv_acc = pd.DataFrame()
        piv_mis = pd.DataFrame()
        profiles = pd.DataFrame()
    else:
        ops = (
            df_operator.groupby("agent_name", dropna=False)
            .agg(Принято=("is_accepted", "sum"), Пропущено=("is_missed", "sum"), Всего=("disposition", "size"))
            .reset_index()
        )
        denom2 = (ops["Принято"] + ops["Пропущено"]).replace(0, pd.NA)
        ops["% пропущенных"] = (100.0 * ops["Пропущено"] / denom2).fillna(0.0).round(2)
        ops = ops.sort_values(["Пропущено", "Принято"], ascending=[False, False])

        piv_acc = (
            df_operator[df_operator["is_accepted"]]
            .pivot_table(index="slot", columns="agent_name", values="is_accepted", aggfunc="sum", fill_value=0)
            .sort_index()
        )
        piv_mis = (
            df_operator[df_operator["is_missed"]]
            .pivot_table(index="slot", columns="agent_name", values="is_missed", aggfunc="sum", fill_value=0)
            .sort_index()
        )

        profiles = build_operator_profiles(df_operator, window)

    skills = (
        df.groupby("skill_name", dropna=False)
        .agg(Принято=("is_accepted", "sum"), Пропущено=("is_missed", "sum"), Всего=("disposition", "size"))
        .reset_index()
        .sort_values(["Пропущено", "Принято"], ascending=[False, False])
    )

    anomalies = build_anomalies(ts)
    peaks = build_peaks(ts)

    return {
        "kpi": kpi,
        "timeseries": ts,
        "operators": ops,
        "operator_profiles": profiles,
        "pivot_accepted": piv_acc,
        "pivot_missed": piv_mis,
        "skills": skills,
        "anomalies": anomalies,
        "peaks": peaks,
        "events": df.sort_values("dt_start").reset_index(drop=True),
    }


def build_operator_profiles(df_operator: pd.DataFrame, window: Optional[Tuple[datetime, datetime]]) -> pd.DataFrame:
    grouped = (
        df_operator.groupby(["agent_code", "agent_name"], dropna=False)
        .agg(
            Принято=("is_accepted", "sum"),
            Пропущено=("is_missed", "sum"),
            Всего=("disposition", "size"),
            **{
                "Первое событие": ("dt_start", "min"),
                "Последнее событие": ("dt_start", "max"),
                "Активных получасов": ("slot", "nunique"),
            },
        )
        .reset_index()
    )
    denom = (grouped["Принято"] + grouped["Пропущено"]).replace(0, pd.NA)
    grouped["% пропущенных"] = (100.0 * grouped["Пропущено"] / denom).fillna(0.0).round(2)

    if window is not None:
        start_dt, end_dt = window
        total_slots = max(1, int((end_dt - start_dt).total_seconds() // 1800))
        grouped["Покрытие по событиям"] = (100.0 * grouped["Активных получасов"] / total_slots).round(1)

        def status(row) -> str:
            coverage = float(row["Покрытие по событиям"])
            if coverage < 20:
                return "Короткое присутствие по событиям"
            if coverage < 55:
                return "Неполное окно по событиям"
            return "Рабочее окно выглядит достаточным"

        grouped["Оценка окна"] = grouped.apply(status, axis=1)
    else:
        grouped["Покрытие по событиям"] = 0.0
        grouped["Оценка окна"] = "Нет общего окна"

    grouped["Первое событие"] = grouped["Первое событие"].dt.strftime("%d.%m %H:%M")
    grouped["Последнее событие"] = grouped["Последнее событие"].dt.strftime("%d.%m %H:%M")

    return grouped.sort_values(["Пропущено", "Принято"], ascending=[False, False])


def build_anomalies(ts: pd.DataFrame) -> pd.DataFrame:
    if ts.empty:
        return pd.DataFrame(columns=["Интервал", "Тип", "Принято", "Пропущено", "Без оператора", "Всего", "Комментарий"])

    rows = []
    for _, row in ts.iterrows():
        accepted = int(row["Принято"])
        missed = int(row["Пропущено"])
        no_agent = int(row["Пропущено_без_оператора"])
        total = int(row["Всего"])

        if missed > 0 and accepted == 0:
            rows.append(
                {
                    "Интервал": slot_interval_label(row["slot"]),
                    "Тип": "Нет принятых при пропусках",
                    "Принято": accepted,
                    "Пропущено": missed,
                    "Без оператора": no_agent,
                    "Всего": total,
                    "Комментарий": "Проверить доступность операторов и маршрутизацию",
                }
            )
        elif no_agent > 0:
            rows.append(
                {
                    "Интервал": slot_interval_label(row["slot"]),
                    "Тип": "Пропущено без оператора",
                    "Принято": accepted,
                    "Пропущено": missed,
                    "Без оператора": no_agent,
                    "Всего": total,
                    "Комментарий": "Отдельно проверить очередь, skill и доступность операторов",
                }
            )

    return pd.DataFrame(rows)


def build_peaks(ts: pd.DataFrame) -> pd.DataFrame:
    if ts.empty:
        return pd.DataFrame(columns=["Интервал", "Всего", "Принято", "Пропущено", "% пропущенных"])

    out = ts.copy()
    denom = (out["Принято"] + out["Пропущено"]).replace(0, pd.NA)
    out["% пропущенных"] = (100.0 * out["Пропущено"] / denom).fillna(0.0).round(2)
    out["Интервал"] = out["slot"].apply(slot_interval_label)
    return out.sort_values(["Всего", "Пропущено"], ascending=[False, False])[
        ["Интервал", "Всего", "Принято", "Пропущено", "% пропущенных"]
    ]


def assess_risk(kpi: Dict[str, float], anomalies: pd.DataFrame, pct_yellow: float, pct_red: float) -> Tuple[str, List[str]]:
    pct = float(kpi.get("% пропущенных", 0.0))
    missed = int(kpi.get("Пропущено", 0))
    no_agent = int(kpi.get("Пропущено без оператора", 0))
    anomaly_count = 0 if anomalies is None or anomalies.empty else len(anomalies)

    level = "Зеленый"
    reasons: List[str] = []

    if missed == 0:
        reasons.append("Пропущенных вызовов нет.")
    if pct >= pct_red:
        level = "Красный"
        reasons.append(f"Процент пропущенных {pct}% выше красного порога {pct_red}%.")
    elif pct >= pct_yellow:
        level = "Желтый"
        reasons.append(f"Процент пропущенных {pct}% выше желтого порога {pct_yellow}%.")

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


def build_summary_lines(
    metrics: Dict[str, pd.DataFrame],
    risk_level: str,
    risk_reasons: List[str],
    window: Optional[Tuple[datetime, datetime]],
    hidden_presence: pd.DataFrame,
    hidden_inactive_codes: Set[str],
) -> List[str]:
    kpi = metrics["kpi"].iloc[0].to_dict()
    lines = []

    if window is not None:
        lines.append(f"Анализируемое окно: {window[0]:%d.%m.%Y %H:%M} - {window[1]:%d.%m.%Y %H:%M}.")
    lines.append(
        f"Итог смены: принято {int(kpi['Принято'])}, пропущено {int(kpi['Пропущено'])}, "
        f"без оператора {int(kpi['Пропущено без оператора'])}, процент пропущенных {kpi['% пропущенных']}%."
    )
    lines.append(f"Риск-статус: {risk_level}. " + " ".join(risk_reasons))

    peaks = metrics.get("peaks", pd.DataFrame())
    if peaks is not None and not peaks.empty:
        top_peak = peaks.iloc[0]
        lines.append(
            f"Пиковая нагрузка: {top_peak['Интервал']}, всего событий {int(top_peak['Всего'])}, "
            f"пропущено {int(top_peak['Пропущено'])}."
        )

    anomalies = metrics.get("anomalies", pd.DataFrame())
    if anomalies is not None and not anomalies.empty:
        intervals = ", ".join(anomalies["Интервал"].head(3).astype(str).tolist())
        lines.append(f"Проблемные интервалы для проверки: {intervals}.")

    if hidden_inactive_codes:
        lines.append(f"Из операторских таблиц исключены неактивные коды: {', '.join(sorted(hidden_inactive_codes))}.")
    if hidden_presence is not None and not hidden_presence.empty:
        codes = ", ".join(hidden_presence["agent_code"].astype(str).head(10).tolist())
        lines.append(f"Короткие человеческие входы не включены в операторские профили: {codes}.")

    return lines


def safe_reindex_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    return df.reindex(columns=cols).fillna(0).astype(int)


def style_missed_matrix(df: pd.DataFrame):
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

    return df.style.applymap(color_value)


def make_excel_report(metrics: Dict[str, pd.DataFrame], summary_lines: List[str], risk_level: str) -> bytes:
    output = io.BytesIO()

    summary_df = pd.DataFrame(
        {
            "Показатель": ["Риск-статус"] + [f"Вывод {i + 1}" for i in range(len(summary_lines))],
            "Значение": [risk_level] + summary_lines,
        }
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Итог")
        for key, sheet_name in [
            ("timeseries", "Динамика"),
            ("operators", "Операторы"),
            ("operator_profiles", "Профили"),
            ("skills", "Тематики"),
            ("anomalies", "Аномалии"),
            ("peaks", "Пики"),
            ("events", "События"),
        ]:
            df = metrics.get(key, pd.DataFrame())
            if df is None or df.empty:
                pd.DataFrame().to_excel(writer, index=False, sheet_name=sheet_name)
            else:
                df.to_excel(writer, index=False, sheet_name=sheet_name)

    return output.getvalue()


def df_to_html(df: pd.DataFrame, limit: int = 200) -> str:
    if df is None or df.empty:
        return "<p>Нет данных</p>"
    return df.head(limit).to_html(index=False, border=1)


def make_word_html_report(metrics: Dict[str, pd.DataFrame], summary_lines: List[str], risk_level: str) -> bytes:
    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <style>
        body {{ font-family: Arial, sans-serif; font-size: 12pt; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 18px; }}
        th, td {{ border: 1px solid #999; padding: 6px; }}
        th {{ background: #f2f2f2; }}
      </style>
    </head>
    <body>
      <h1>Анализ смены Avaya CMS</h1>
      <h2>Риск-статус: {risk_level}</h2>
      <h2>Автоматический вывод</h2>
      <ul>{''.join(f'<li>{line}</li>' for line in summary_lines)}</ul>
      <h2>KPI</h2>{df_to_html(metrics.get('kpi'))}
      <h2>Аномалии</h2>{df_to_html(metrics.get('anomalies'))}
      <h2>Операторы</h2>{df_to_html(metrics.get('operators'))}
      <h2>Профили операторов</h2>{df_to_html(metrics.get('operator_profiles'))}
      <h2>Тематики</h2>{df_to_html(metrics.get('skills'))}
      <h2>Пики нагрузки</h2>{df_to_html(metrics.get('peaks'))}
    </body>
    </html>
    """
    return html.encode("utf-8")


# UI
st.set_page_config(page_title="Avaya CMS: анализ смен", layout="wide")
st.title("Avaya CMS — анализ смен")

if "mode" not in st.session_state:
    st.session_state.mode = "night"

if "agent_table" not in st.session_state or "skill_table" not in st.session_state:
    settings = load_settings()
    st.session_state.agent_table = settings["agents"]
    st.session_state.skill_table = settings["skills"]

with st.sidebar:
    st.header("Файл")
    html_file = st.file_uploader("HTML отчет Avaya", type=["html", "htm"])

    st.divider()
    st.header("Смена")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Ночь", use_container_width=True):
            st.session_state.mode = "night"
    with c2:
        if st.button("День", use_container_width=True):
            st.session_state.mode = "day"

    mode = st.radio(
        "Режим",
        options=["night", "day", "custom"],
        format_func=lambda x: SHIFT_PRESETS[x].name,
        index=["night", "day", "custom"].index(st.session_state.mode),
    )
    st.session_state.mode = mode

    if mode == "custom":
        st.caption("Если конец меньше начала, смена идет через полночь.")
        sh = st.number_input("Старт: час", 0, 23, NIGHT_START[0])
        sm = st.number_input("Старт: мин", 0, 59, NIGHT_START[1])
        eh = st.number_input("Конец: час", 0, 23, NIGHT_END[0])
        em = st.number_input("Конец: мин", 0, 59, NIGHT_END[1])
        SHIFT_PRESETS["custom"] = ShiftSpec("Кастом", (int(sh), int(sm)), (int(eh), int(em)), None)

    shift = SHIFT_PRESETS[mode]

    st.divider()
    st.header("Правила анализа")
    treat_conn_as_accepted = st.checkbox("Считать CONN как принято", value=False)
    only_watched_for_missed = st.checkbox("Пропущенные считать только по выбранным skills", value=(mode == "night"))

    skill_table_current = normalize_skill_table(st.session_state.skill_table)
    skill_options = skill_table_current["skill_code"].astype(str).tolist()
    watched_default = watched_skills_from_table(skill_table_current)
    watched_skills = st.multiselect("Skills для пропущенных", options=skill_options, default=watched_default)

    pct_yellow = st.number_input("Желтый порог % пропущенных", min_value=0.0, max_value=100.0, value=5.0, step=0.5)
    pct_red = st.number_input("Красный порог % пропущенных", min_value=0.0, max_value=100.0, value=10.0, step=0.5)

    st.divider()
    st.header("Человеческий фактор")
    exclude_inactive = st.checkbox("Скрывать неактивных операторов из профилей и матриц", value=True)
    enable_presence_guard = st.checkbox("Не считать короткий вход полноценной сменой", value=True)
    min_active_slots = st.number_input("Минимум активных получасов в профиле", min_value=1, max_value=24, value=2, step=1)

    st.divider()
    st.header("Наглядность")
    top_n = st.slider("Топ N", 3, 30, 10)
    max_pivot_cols = st.slider("Макс операторов в матрице", 3, 60, 15)
    show_raw = st.checkbox("Показывать сырые события", value=False)

    st.divider()
    st.header("Справочники")
    show_settings = st.checkbox("Показать редактор операторов и skills", value=False)

    if show_settings:
        st.subheader("Операторы")
        uploaded_mapping = st.file_uploader("Загрузить операторов CSV/XLSX", type=["csv", "txt", "xlsx", "xls"])
        pasted = st.text_area("Вставить строки вида: Оператор - Фамилия ИО (код)", height=120)

        m1, m2, m3 = st.columns(3)
        with m1:
            if st.button("Добавить из файла", use_container_width=True) and uploaded_mapping is not None:
                st.session_state.agent_table = merge_agent_tables(st.session_state.agent_table, load_mapping_file(uploaded_mapping))
        with m2:
            if st.button("Добавить из текста", use_container_width=True) and pasted:
                st.session_state.agent_table = merge_agent_tables(st.session_state.agent_table, extract_agents_from_registry_text(pasted))
        with m3:
            if st.button("Сброс операторов", use_container_width=True):
                st.session_state.agent_table = default_agent_table()

        edited_agents = st.data_editor(
            normalize_agent_table(st.session_state.agent_table),
            use_container_width=True,
            num_rows="dynamic",
            key="agent_editor",
        )
        st.session_state.agent_table = normalize_agent_table(edited_agents)

        st.subheader("Skills")
        edited_skills = st.data_editor(
            normalize_skill_table(st.session_state.skill_table),
            use_container_width=True,
            num_rows="dynamic",
            key="skill_editor",
        )
        st.session_state.skill_table = normalize_skill_table(edited_skills)

        s1, s2 = st.columns(2)
        with s1:
            if st.button("Сохранить настройки", use_container_width=True):
                ok, msg = save_settings(st.session_state.agent_table, st.session_state.skill_table)
                st.success(msg) if ok else st.error(msg)
        with s2:
            if st.button("Скачать операторов CSV", use_container_width=True):
                st.download_button(
                    "Скачать файл",
                    data=normalize_agent_table(st.session_state.agent_table).to_csv(index=False).encode("utf-8"),
                    file_name="agent_mapping.csv",
                    mime="text/csv",
                )

if html_file is None:
    st.info("Загрузи HTML отчет Avaya слева. После загрузки появится анализ смены.")
    st.stop()

df_raw = parse_avaya_html(html_file.getvalue())

if df_raw.empty:
    st.error("Не удалось найти таблицу Avaya в HTML-файле.")
    st.stop()

found_cols = detect_avaya_columns(df_raw)
missing_cols = missing_required_columns(found_cols)
if missing_cols:
    st.error("Не хватает обязательных колонок: " + ", ".join(missing_cols))
    st.caption("Найденные колонки: " + ", ".join([str(c) for c in df_raw.columns]))
    st.stop()

df = normalize_calls(df_raw)

if df.empty:
    st.error("Таблица найдена, но строки не распознаны. Проверь формат даты и времени в выгрузке Avaya.")
    st.caption("Найденные колонки: " + ", ".join([str(c) for c in df_raw.columns]))
    st.stop()

agent_table = normalize_agent_table(st.session_state.agent_table)
skill_table = normalize_skill_table(st.session_state.skill_table)
agent_map = agent_map_from_table(agent_table)
skill_names = skill_name_map_from_table(skill_table)

df = apply_agent_map(df, agent_map)

dates = available_shift_dates(df, shift)
if len(dates) <= 1:
    base_date = dates[0] if dates else None
    if base_date:
        st.caption(f"В файле найдена 1 смена для режима: {base_date:%d.%m.%Y}")
else:
    options = ["Все смены в файле"] + [d.strftime("%d.%m.%Y") for d in dates]
    chosen = st.selectbox("Дата смены, если в HTML несколько дней:", options=options, index=0)
    base_date = None if chosen == "Все смены в файле" else datetime.strptime(chosen, "%d.%m.%Y").date()

df_shift, window = filter_by_shift(df, shift, base_date)
if df_shift.empty:
    st.warning("В выбранном окне смены нет событий. Попробуй другую дату или режим.")
    st.stop()

hidden_codes: Set[str] = set()
hidden_inactive_codes: Set[str] = set()
if exclude_inactive:
    hidden_inactive_codes = inactive_agent_codes(agent_table)
    hidden_codes |= hidden_inactive_codes

hidden_presence = pd.DataFrame()
if enable_presence_guard:
    short_codes, hidden_presence = detect_short_presence_agents(df_shift, int(min_active_slots))
    hidden_codes |= short_codes

metrics = compute_metrics(
    df_shift=df_shift,
    watched_skills=[str(x) for x in watched_skills],
    only_watched_for_missed=only_watched_for_missed,
    treat_conn_as_accepted=treat_conn_as_accepted,
    skill_names=skill_names,
    hidden_agent_codes=hidden_codes,
    window=window,
)

kpi = metrics["kpi"].iloc[0].to_dict()
risk_level, risk_reasons = assess_risk(kpi, metrics["anomalies"], float(pct_yellow), float(pct_red))
summary_lines = build_summary_lines(metrics, risk_level, risk_reasons, window, hidden_presence, hidden_inactive_codes)

if window is not None:
    st.subheader(f"Окно анализа: {window[0]:%d.%m.%Y %H:%M} - {window[1]:%d.%m.%Y %H:%M} ({shift.name})")
else:
    st.subheader(f"Окно анализа: {shift.name}")

with st.expander("Проверка входного файла", expanded=False):
    col_report = pd.DataFrame(
        [{"Поле": k, "Колонка в файле": v or "Не найдена"} for k, v in found_cols.items()]
    )
    st.dataframe(col_report, use_container_width=True)
    st.caption(f"Строк в HTML: {len(df_raw)}. Распознано событий: {len(df)}.")

st.markdown("### Автоматический вывод")
if risk_level == "Красный":
    st.error(f"Риск-статус: {risk_level}")
elif risk_level == "Желтый":
    st.warning(f"Риск-статус: {risk_level}")
else:
    st.success(f"Риск-статус: {risk_level}")

for line in summary_lines:
    st.write("• " + line)

st.markdown("### KPI")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Принято", int(kpi["Принято"]))
c2.metric("Пропущено", int(kpi["Пропущено"]))
c3.metric("Без оператора", int(kpi["Пропущено без оператора"]))
c4.metric("% пропущенных", f"{kpi['% пропущенных']}%")
c5.metric("Всего", int(kpi["Всего событий"]))

st.markdown("### Динамика по получасам")
if not metrics["timeseries"].empty:
    ts_chart = metrics["timeseries"].set_index("slot")[["Принято", "Пропущено", "Пропущено_без_оператора"]]
    st.line_chart(ts_chart)
else:
    st.caption("Нет данных для динамики.")

st.markdown("### Пики нагрузки")
st.dataframe(metrics["peaks"].head(top_n), use_container_width=True)

st.markdown("### Аномалии")
st.dataframe(metrics["anomalies"], use_container_width=True)

st.markdown("### Операторы")
colA, colB = st.columns(2)
with colA:
    st.markdown(f"**Топ-{top_n} операторов по пропущенным**")
    st.dataframe(metrics["operators"].head(top_n), use_container_width=True)
with colB:
    st.markdown(f"**Топ-{top_n} тематик по пропущенным**")
    st.dataframe(metrics["skills"].head(top_n), use_container_width=True)

st.markdown("### Профиль операторов")
st.caption("Профиль строится по фактическим событиям оператора в файле, а не по предположению, что он был в системе всю смену.")
st.dataframe(metrics["operator_profiles"], use_container_width=True)

if hidden_presence is not None and not hidden_presence.empty:
    with st.expander("Короткие входы, исключенные из операторских профилей"):
        st.dataframe(hidden_presence, use_container_width=True)

st.markdown("### Матрица по получасам")
piv_acc = metrics["pivot_accepted"]
piv_mis = metrics["pivot_missed"]
all_ops = sorted(
    set(
        (list(piv_acc.columns) if not piv_acc.empty else [])
        + (list(piv_mis.columns) if not piv_mis.empty else [])
        + (metrics["operators"]["agent_name"].tolist() if not metrics["operators"].empty else [])
    )
)
default_ops = (metrics["operators"]["agent_name"].tolist() if not metrics["operators"].empty else [])[:max_pivot_cols]
default_ops = [op for op in default_ops if op in all_ops][:max_pivot_cols]
selected_ops = st.multiselect("Выбери операторов для матрицы:", options=all_ops, default=default_ops)

if selected_ops:
    acc_show = safe_reindex_columns(piv_acc, selected_ops)
    mis_show = safe_reindex_columns(piv_mis, selected_ops)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Принятые**")
        st.dataframe(acc_show, use_container_width=True)
    with c2:
        st.markdown("**Пропущенные с подсветкой**")
        st.dataframe(style_missed_matrix(mis_show), use_container_width=True)
else:
    st.caption("Выбери хотя бы одного оператора, чтобы показать матрицу.")

st.markdown("### Экспорт")
events = metrics["events"]

excel_bytes = make_excel_report(metrics, summary_lines, risk_level)
word_bytes = make_word_html_report(metrics, summary_lines, risk_level)

e1, e2, e3 = st.columns(3)
with e1:
    st.download_button(
        "Скачать Excel-отчет",
        data=excel_bytes,
        file_name="avaya_shift_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
with e2:
    st.download_button(
        "Скачать Word-отчет",
        data=word_bytes,
        file_name="avaya_shift_report.doc",
        mime="application/msword",
        use_container_width=True,
    )
with e3:
    st.download_button(
        "Скачать CSV событий",
        data=events.to_csv(index=False).encode("utf-8"),
        file_name="avaya_shift_events.csv",
        mime="text/csv",
        use_container_width=True,
    )

if show_raw:
    st.markdown("### Сырые события")
    st.dataframe(events.head(2000), use_container_width=True)
