# app_minimal.py — компактный резервный анализатор Avaya
# Быстрый режим из минимальной версии: ANS+CONN принято, ABAN пропущено.

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup

ACCEPTED = {"ANS", "CONN"}
MISSED = {"ABAN"}
DEFAULT_SKILLS = ["1", "3", "9"]
SKILL_NAMES = {"1": "Надежность", "3": "Качество э/э", "9": "ЭЗС"}


@dataclass(frozen=True)
class ShiftSpec:
    name: str
    start_hm: Tuple[int, int]
    end_hm: Tuple[int, int]

    def bounds(self, base: date):
        start = datetime.combine(base, time(*self.start_hm))
        end = datetime.combine(base, time(*self.end_hm))
        if end <= start:
            end += timedelta(days=1)
        return start, end


SHIFT_NIGHT = ShiftSpec("Ночь (18:30-06:30)", (18, 30), (6, 30))
SHIFT_DAY = ShiftSpec("День (06:30-18:30)", (6, 30), (18, 30))


def decode_bytes(data: bytes) -> str:
    for enc in ("cp1251", "windows-1251", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass
    return data.decode("utf-8", errors="replace")


def clean(value) -> str:
    return ("" if value is None else str(value)).replace("\xa0", " ").strip()


def best_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None

    def score(tbl):
        headers = [clean(x.get_text(" ", strip=True)).lower() for x in tbl.find_all("th")]
        keys = ("дата", "время", "размещение", "split/skill", "имена пользователей")
        return sum(any(k in h for h in headers) for k in keys) * 10 + len(headers)

    return max(tables, key=score)


@st.cache_data(show_spinner=False)
def parse_html(data: bytes) -> pd.DataFrame:
    soup = BeautifulSoup(decode_bytes(data), "html.parser")
    table = best_table(soup)
    if table is None:
        return pd.DataFrame()
    headers = [clean(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    if not headers:
        return pd.DataFrame()
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row = [clean(td.get_text(" ", strip=True)) for td in cells]
        row = (row + [""] * len(headers))[: len(headers)]
        rows.append(row)
    return pd.DataFrame(rows, columns=headers)


def find_col(df: pd.DataFrame, names):
    lower = {str(c).lower().strip(): c for c in df.columns}
    for n in names:
        hit = lower.get(n.lower().strip())
        if hit is not None:
            return hit
    return None


def normalize(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    date_col = find_col(raw, ["Дата", "Date"])
    time_col = find_col(raw, ["Время нач.", "Время нач", "Time"])
    disp_col = find_col(raw, ["Размещение", "Disposition"])
    skill_col = find_col(raw, ["Split/Skill", "Skill", "Split"])
    agent_col = find_col(raw, ["Имена пользователей", "Agent", "Пользователь", "User"])
    if not (date_col and time_col and disp_col):
        return pd.DataFrame()

    df = raw.copy()
    dt_text = (df[date_col].astype(str).str.strip() + " " + df[time_col].astype(str).str.strip()).str.strip()
    df["dt_start"] = pd.to_datetime(dt_text, format="%d.%m.%Y %H:%M:%S", errors="coerce")
    if df["dt_start"].isna().all():
        df["dt_start"] = pd.to_datetime(dt_text, dayfirst=True, errors="coerce")
    df = df.dropna(subset=["dt_start"]).reset_index(drop=True)
    if df.empty:
        return pd.DataFrame()

    df["disposition"] = df[disp_col].astype(str).str.strip().str.upper()
    df["skill_raw"] = "" if skill_col is None else df[skill_col].astype(str).str.strip()
    df["skill_code"] = df["skill_raw"].str.extract(r"(\d+)", expand=False).fillna("")
    df["agent"] = "" if agent_col is None else df[agent_col].astype(str).str.strip()
    df.loc[df["agent"].isin(["nan", "None"]), "agent"] = ""
    df["agent_show"] = df["agent"].replace("", "Без оператора")
    df["call_date"] = df["dt_start"].dt.date
    return df


def available_dates(df: pd.DataFrame, shift: ShiftSpec):
    dates = set(df["call_date"].dropna().tolist())
    if not dates:
        return []
    sample = next(iter(dates))
    s, e = shift.bounds(sample)
    if e.date() > s.date():
        dates |= {d - timedelta(days=1) for d in dates}
    out = []
    for d in sorted(dates):
        s, e = shift.bounds(d)
        if ((df["dt_start"] >= s) & (df["dt_start"] < e)).any():
            out.append(d)
    return out


def filter_shift(df: pd.DataFrame, shift: ShiftSpec, base: Optional[date]):
    dates = available_dates(df, shift)
    if not dates:
        return df.iloc[0:0].copy(), None
    if base is None:
        parts = []
        for d in dates:
            s, e = shift.bounds(d)
            parts.append(df[(df["dt_start"] >= s) & (df["dt_start"] < e)])
        return pd.concat(parts, ignore_index=True), (shift.bounds(dates[0])[0], shift.bounds(dates[-1])[1])
    s, e = shift.bounds(base)
    return df[(df["dt_start"] >= s) & (df["dt_start"] < e)].copy(), (s, e)


def calculate(df: pd.DataFrame, watched_skills):
    data = df.copy()
    data["accepted"] = data["disposition"].isin(ACCEPTED)
    data["missed"] = data["disposition"].isin(MISSED) & data["skill_code"].isin(watched_skills)
    data["missed_no_agent"] = data["missed"] & (data["agent"].astype(str).str.strip() == "")
    data["topic"] = data["skill_code"].map(SKILL_NAMES).fillna(data["skill_code"].replace("", "Без skill"))

    accepted = int(data["accepted"].sum())
    missed = int(data["missed"].sum())
    denom = accepted + missed
    kpi = pd.DataFrame([{
        "Принято (ANS+CONN)": accepted,
        "Пропущено (ABAN)": missed,
        "Без оператора": int(data["missed_no_agent"].sum()),
        "% пропущенных": round(100 * missed / denom, 2) if denom else 0.0,
        "Всего событий": int(len(data)),
    }])
    operators = data[data["agent"] != ""].groupby("agent_show").agg(Принято=("accepted", "sum"), Пропущено=("missed", "sum"), Всего=("disposition", "size")).reset_index().sort_values(["Пропущено", "Принято"], ascending=[False, False])
    topics = data.groupby("topic").agg(Принято=("accepted", "sum"), Пропущено=("missed", "sum"), Всего=("disposition", "size")).reset_index().sort_values(["Пропущено", "Принято"], ascending=[False, False])
    statuses = data.groupby("disposition").size().reset_index(name="count").sort_values("count", ascending=False)
    return {"kpi": kpi, "operators": operators, "topics": topics, "statuses": statuses, "events": data.sort_values("dt_start")}


st.set_page_config(page_title="Avaya: быстрый анализ", layout="wide")
st.title("Avaya Call Records — быстрый анализ")
st.caption("Минимальный резервный режим: ANS+CONN принято, ABAN пропущено по выбранным skills.")

with st.sidebar:
    html = st.file_uploader("HTML отчет Avaya", type=["html", "htm"])
    mode = st.radio("Смена", ["Ночь", "День"], horizontal=True)
    shift = SHIFT_NIGHT if mode == "Ночь" else SHIFT_DAY
    watched = st.multiselect("Skills для ABAN", DEFAULT_SKILLS, default=DEFAULT_SKILLS)
    top_n = st.slider("Топ N", 3, 30, 10)
    show_events = st.checkbox("Показать события", value=False)

if html is None:
    st.info("Загрузи HTML отчет Avaya слева.")
    st.stop()

raw = parse_html(html.getvalue())
df = normalize(raw)
if df.empty:
    st.error("Не удалось распознать отчет. Нужны колонки: Дата, Время нач., Размещение.")
    st.stop()

dates = available_dates(df, shift)
options = ["Все смены в файле"] + [d.strftime("%d.%m.%Y") for d in dates]
chosen = st.selectbox("Дата смены", options)
base = None if chosen == "Все смены в файле" else datetime.strptime(chosen, "%d.%m.%Y").date()

df_shift, window = filter_shift(df, shift, base)
if df_shift.empty:
    st.warning("В выбранном окне смены нет событий.")
    st.stop()

metrics = calculate(df_shift, watched)
kpi = metrics["kpi"].iloc[0].to_dict()

if window:
    st.subheader(f"Окно анализа: {window[0]:%d.%m.%Y %H:%M} - {window[1]:%d.%m.%Y %H:%M} ({shift.name})")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Принято", int(kpi["Принято (ANS+CONN)"]))
c2.metric("Пропущено", int(kpi["Пропущено (ABAN)"]))
c3.metric("Без оператора", int(kpi["Без оператора"]))
c4.metric("% пропущенных", f"{kpi['% пропущенных']}%")
c5.metric("Всего", int(kpi["Всего событий"]))
st.caption("Формула: % пропущенных = ABAN / (ANS + CONN + ABAN) по выбранным skills.")

with st.expander("Статусы"):
    st.dataframe(metrics["statuses"], use_container_width=True)

left, right = st.columns(2)
with left:
    st.markdown(f"### Топ-{top_n} операторов")
    st.dataframe(metrics["operators"].head(top_n), use_container_width=True)
with right:
    st.markdown(f"### Топ-{top_n} тематик")
    st.dataframe(metrics["topics"].head(top_n), use_container_width=True)

st.download_button("Скачать CSV событий", metrics["events"].to_csv(index=False).encode("utf-8-sig"), "avaya_shift_events.csv", "text/csv", use_container_width=True)

if show_events:
    st.markdown("### События")
    st.dataframe(metrics["events"].head(2000), use_container_width=True)
