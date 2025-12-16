
# app_avaya_shift_analyzer_v2.py
# Универсальный анализатор Avaya CMS Call Records (HTML) для НОЧНЫХ и ДНЕВНЫХ смен.
#
# Запуск:
#   pip install streamlit pandas beautifulsoup4 lxml openpyxl
#   streamlit run app_avaya_shift_analyzer_v2.py
#
# Что умеет:
# - Кнопки: "Ночь" / "День" (и "Кастом")
# - Если в HTML несколько дней (как у вас 12.12–15.12), можно выбрать дату смены или "Все смены"
# - Сводка KPI, график по получасам, топ операторов/тематик, матрица получас×оператор
# - Встроенный список операторов (по вашим скринам) + возможность добавлять новых:
#   1) загрузкой CSV/XLSX (код -> имя)
#   2) вставкой текста из реестра (парсим только строки "Оператор - ... (7599...)")
#   3) ручным добавлением в таблице (Data editor) и скачиванием CSV
#
# Важно:
# - "Принято" по умолчанию = ANS (опция считать CONN как принято)
# - "Пропущено" по умолчанию = ABAN (опция считать пропущено только по выбранным тематикам)

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


# -----------------------------
# Базовые настройки
# -----------------------------

WATCHED_SKILLS_DEFAULT = ["1", "3", "9"]
SKILL_NAMES_DEFAULT = {
    "1": "Надежность",
    "3": "Качество э/э",
    "9": "ЭЗС",
}

# Ночь (как было)
NIGHT_START = (18, 30)
NIGHT_END = (6, 30)

# День (по умолчанию)
DAY_START = (6, 30)
DAY_END = (18, 30)

DEFAULT_LEFTOVER_MIN = 60

DISP_ACCEPTED = {"ANS"}  # + optional CONN
DISP_MISSED = {"ABAN"}

# --- Встроенный маппинг операторов (по вашим скриншотам) ---
# Можно дополнять прямо тут новыми строками "код": "Фамилия ИО"
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


@dataclass(frozen=True)
class ShiftSpec:
    name: str
    start_hm: Tuple[int, int]
    end_hm: Tuple[int, int]
    crosses_midnight: Optional[bool] = None  # None -> авто (end<=start)

    def bounds(self, base_date: date) -> Tuple[datetime, datetime]:
        start_dt = datetime.combine(base_date, time(self.start_hm[0], self.start_hm[1]))
        end_dt = datetime.combine(base_date, time(self.end_hm[0], self.end_hm[1]))
        crosses = (end_dt <= start_dt) if self.crosses_midnight is None else self.crosses_midnight
        if crosses:
            end_dt += timedelta(days=1)
        return start_dt, end_dt


SHIFT_PRESETS = {
    "Ночная (18:30–06:30)": ShiftSpec("Ночная (18:30–06:30)", NIGHT_START, NIGHT_END, None),
    "Дневная (06:30–18:30)": ShiftSpec("Дневная (06:30–18:30)", DAY_START, DAY_END, False),
    "Кастом": ShiftSpec("Кастом", NIGHT_START, NIGHT_END, None),  # перезапишем из UI
}


# -----------------------------
# Парсинг HTML Avaya
# -----------------------------

def _safe_decode(data: bytes) -> str:
    for enc in ("cp1251", "windows-1251", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _clean_cell_text(x: str) -> str:
    x = x.replace("\xa0", " ").strip()
    return "" if x in {"&nbsp;", "\u00a0"} else x


def _find_target_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None

    def score(tbl) -> int:
        ths = [(_clean_cell_text(th.get_text(" ", strip=True)) or "").lower() for th in tbl.find_all("th")]
        s = 0
        for key in ("id вызова", "размещение", "split/skill", "имена пользователей", "время нач"):
            if any(key in th for th in ths):
                s += 2
        s += min(len(ths), 30) // 5
        return s

    best = max(tables, key=score)
    return best if best.find_all("th") else None


@st.cache_data(show_spinner=False)
def parse_avaya_html_to_df(html_bytes: bytes) -> pd.DataFrame:
    try:
        text = _safe_decode(html_bytes)
        soup = BeautifulSoup(text, "html.parser")
        tbl = _find_target_table(soup)
        if tbl is None:
            return pd.DataFrame()

        headers = [_clean_cell_text(th.get_text(" ", strip=True)) for th in tbl.find_all("th")]
        rows = []
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            row = [_clean_cell_text(td.get_text(" ", strip=True)) for td in tds]
            if len(row) < len(headers):
                row += [""] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            rows.append(row)

        return pd.DataFrame(rows, columns=headers)
    except Exception:
        return pd.DataFrame()


def _first_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def parse_datetime_columns(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    col_date = _first_existing_col(df, ["Дата", "Date"])
    col_time = _first_existing_col(df, ["Время нач.", "Время нач", "Time"])
    col_disp = _first_existing_col(df, ["Размещение", "Disposition"])
    col_skill = _first_existing_col(df, ["Split/Skill", "Skill", "Split"])
    col_agent = _first_existing_col(df, ["Имена пользователей", "Agent", "Пользователь", "User"])

    if not (col_date and col_time and col_disp):
        return pd.DataFrame()

    dt_str = (df[col_date].astype(str).str.strip() + " " + df[col_time].astype(str).str.strip()).str.strip()
    df["dt_start"] = pd.to_datetime(dt_str, format="%d.%m.%Y %H:%M:%S", errors="coerce")
    df = df.dropna(subset=["dt_start"]).reset_index(drop=True)

    df["disposition"] = df[col_disp].astype(str).str.strip().str.upper()
    df["skill_raw"] = "" if col_skill is None else df[col_skill].astype(str).str.strip()
    df["agent_code"] = "" if col_agent is None else df[col_agent].astype(str).str.strip()

    df["skill_code"] = df["skill_raw"].str.extract(r"(\d+)", expand=False).fillna("").astype(str)
    df["call_date"] = df["dt_start"].dt.date
    return df


def floor_to_half_hour(ts: pd.Timestamp) -> pd.Timestamp:
    minute = 0 if ts.minute < 30 else 30
    return ts.replace(minute=minute, second=0, microsecond=0)


def available_shift_dates(df: pd.DataFrame, shift: ShiftSpec) -> List[date]:
    if df.empty:
        return []
    base_dates = sorted({d for d in df["call_date"].dropna().tolist()})
    good: List[date] = []
    for bd in base_dates:
        st_dt, en_dt = shift.bounds(bd)
        m = (df["dt_start"] >= pd.Timestamp(st_dt)) & (df["dt_start"] < pd.Timestamp(en_dt))
        if m.any():
            good.append(bd)
    return good


def filter_by_shift(df: pd.DataFrame, shift: ShiftSpec, base_date: Optional[date]) -> Tuple[pd.DataFrame, Optional[Tuple[datetime, datetime]]]:
    if df.empty:
        return df, None

    if base_date is None:
        dates = available_shift_dates(df, shift)
        if not dates:
            return df.iloc[0:0].copy(), None
        parts = []
        for d in dates:
            st_dt, en_dt = shift.bounds(d)
            m = (df["dt_start"] >= pd.Timestamp(st_dt)) & (df["dt_start"] < pd.Timestamp(en_dt))
            parts.append(df.loc[m])
        out = pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()
        st_dt = shift.bounds(dates[0])[0]
        en_dt = shift.bounds(dates[-1])[1]
        return out, (st_dt, en_dt)

    st_dt, en_dt = shift.bounds(base_date)
    m = (df["dt_start"] >= pd.Timestamp(st_dt)) & (df["dt_start"] < pd.Timestamp(en_dt))
    return df.loc[m].copy().reset_index(drop=True), (st_dt, en_dt)


def drop_leftovers(df_shift: pd.DataFrame, shift_window: Tuple[datetime, datetime], minutes: int) -> Tuple[pd.DataFrame, List[str]]:
    if df_shift.empty:
        return df_shift, []
    st_dt, _ = shift_window
    cutoff = st_dt + timedelta(minutes=minutes)

    agents = df_shift.loc[df_shift["agent_code"].astype(str).str.strip() != "", "agent_code"].astype(str).unique().tolist()
    excluded: List[str] = []
    for a in agents:
        df_a = df_shift[df_shift["agent_code"] == a]
        if df_a.empty:
            continue
        first = df_a["dt_start"].min()
        last = df_a["dt_start"].max()
        if first < cutoff and last < cutoff:
            excluded.append(a)

    if excluded:
        df_shift = df_shift[~df_shift["agent_code"].isin(excluded)].copy().reset_index(drop=True)
    return df_shift, excluded


# -----------------------------
# Маппинг операторов: загрузка / вставка текста / редактирование
# -----------------------------

def load_agent_mapping(uploaded) -> Dict[str, str]:
    """CSV/XLSX -> dict(code->name). Никаких падений."""
    if uploaded is None:
        return {}
    try:
        name = getattr(uploaded, "name", "").lower()
        data = uploaded.getvalue()
        if name.endswith(".csv") or name.endswith(".txt"):
            df = None
            for sep in [",", ";", "\t"]:
                try:
                    df = pd.read_csv(io.BytesIO(data), sep=sep, dtype=str)
                    if df.shape[1] >= 2:
                        break
                except Exception:
                    df = None
            if df is None:
                return {}
        else:
            df = pd.read_excel(io.BytesIO(data), dtype=str)

        df = df.fillna("")
        # первые две колонки: code, name
        code_col, name_col = df.columns[0], df.columns[1]
        mapping: Dict[str, str] = {}
        for _, r in df.iterrows():
            code = str(r[code_col]).strip()
            nm = str(r[name_col]).strip()
            if code and nm:
                mapping[code] = nm
        return mapping
    except Exception:
        return {}


_RE_REGISTRY_LINE = re.compile(
    r"^\s*Оператор\s*-\s*(?P<name>.+?)\s*\((?P<code>\d{4,})\)\s*$",
    re.IGNORECASE
)

def parse_registry_text_to_mapping(text: str) -> Dict[str, str]:
    """
    Парсим вставленный текст из реестра.
    Учитываем ТОЛЬКО строки "Оператор - ... (код)".
    Строки "Телефон - ... (код станции)" игнорируются автоматически.
    """
    if not text:
        return {}
    mapping: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = _RE_REGISTRY_LINE.match(line)
        if not m:
            continue
        name = m.group("name").strip()
        code = m.group("code").strip()
        # убираем двойные пробелы
        name = re.sub(r"\s+", " ", name)
        mapping[code] = name
    return mapping


def apply_agent_names(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    df["agent_name"] = df["agent_code"].astype(str).map(mapping).fillna(df["agent_code"].astype(str))
    return df


# -----------------------------
# Аналитика
# -----------------------------

def compute_metrics(
    df_shift: pd.DataFrame,
    watched_skills: List[str],
    only_watched_for_missed: bool,
    treat_conn_as_accepted: bool,
) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    if df_shift.empty:
        out["events"] = df_shift
        return out

    df = df_shift.copy()

    accepted_set = set(DISP_ACCEPTED) | ({"CONN"} if treat_conn_as_accepted else set())
    df["is_accepted"] = df["disposition"].isin(accepted_set)
    df["is_missed"] = df["disposition"].isin(DISP_MISSED)

    if only_watched_for_missed and watched_skills:
        df["is_missed"] = df["is_missed"] & df["skill_code"].isin(watched_skills)

    df["is_missed_no_agent"] = df["is_missed"] & (df["agent_code"].astype(str).str.strip() == "")
    df["slot"] = df["dt_start"].apply(lambda x: floor_to_half_hour(pd.Timestamp(x)))

    kpi = pd.DataFrame(
        [{
            "Принято": int(df["is_accepted"].sum()),
            "Пропущено": int(df["is_missed"].sum()),
            "Пропущено без оператора": int(df["is_missed_no_agent"].sum()),
            "Всего событий": int(len(df)),
        }]
    )
    denom = float(kpi["Принято"].iloc[0] + kpi["Пропущено"].iloc[0])
    kpi["% пропущенных"] = 0.0 if denom == 0 else round(100.0 * float(kpi["Пропущено"].iloc[0]) / denom, 2)
    out["kpi"] = kpi

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
    out["timeseries"] = ts

    df_with_agent = df[df["agent_code"].astype(str).str.strip() != ""].copy()
    if df_with_agent.empty:
        ops = pd.DataFrame(columns=["agent_name", "Принято", "Пропущено", "% пропущенных"])
    else:
        ops = (
            df_with_agent.groupby(["agent_name"], dropna=False)
            .agg(Принято=("is_accepted", "sum"), Пропущено=("is_missed", "sum"), Всего=("disposition", "size"))
            .reset_index()
        )
        denom2 = (ops["Принято"] + ops["Пропущено"]).replace(0, pd.NA)
        ops["% пропущенных"] = (100.0 * ops["Пропущено"] / denom2).fillna(0.0).round(2)
        ops = ops.sort_values(["Пропущено", "Принято"], ascending=[False, False])
    out["operators"] = ops

    if df_with_agent.empty:
        out["pivot_accepted"] = pd.DataFrame()
        out["pivot_missed"] = pd.DataFrame()
    else:
        out["pivot_accepted"] = (
            df_with_agent[df_with_agent["is_accepted"]]
            .pivot_table(index="slot", columns="agent_name", values="is_accepted", aggfunc="sum", fill_value=0)
            .sort_index()
        )
        out["pivot_missed"] = (
            df_with_agent[df_with_agent["is_missed"]]
            .pivot_table(index="slot", columns="agent_name", values="is_missed", aggfunc="sum", fill_value=0)
            .sort_index()
        )

    skill_map = dict(SKILL_NAMES_DEFAULT)
    df["skill_name"] = df["skill_code"].map(skill_map).fillna(df["skill_code"].replace("", "Без тематики"))
    out["skill_summary"] = (
        df.groupby(["skill_name"], dropna=False)
        .agg(Принято=("is_accepted", "sum"), Пропущено=("is_missed", "sum"), Всего=("disposition", "size"))
        .reset_index()
        .sort_values(["Пропущено", "Принято"], ascending=[False, False])
    )

    bad_slots = ts[(ts["Пропущено"] > 0) & (ts["Принято"] == 0)][["slot", "Пропущено", "Пропущено_без_оператора", "Всего"]]
    out["bad_slots"] = bad_slots

    out["events"] = df.sort_values("dt_start").reset_index(drop=True)
    return out


# -----------------------------
# UI
# -----------------------------

st.set_page_config(page_title="Avaya CMS: анализ смен", layout="wide")
st.title("Avaya CMS — анализ смен (ночь / день)")

if "shift_mode" not in st.session_state:
    st.session_state["shift_mode"] = "Ночная (18:30–06:30)"

with st.sidebar:
    st.header("1) Файлы")
    html_file = st.file_uploader("HTML отчёт Avaya", type=["html", "htm"])
    mapping_file = st.file_uploader("Доп. маппинг операторов (CSV/XLSX) — опционально", type=["csv", "txt", "xlsx", "xls"])

    st.divider()
    st.header("2) Смена")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Ночь", use_container_width=True):
            st.session_state["shift_mode"] = "Ночная (18:30–06:30)"
    with c2:
        if st.button("День", use_container_width=True):
            st.session_state["shift_mode"] = "Дневная (06:30–18:30)"

    shift_mode = st.radio(
        "Режим",
        options=list(SHIFT_PRESETS.keys()),
        index=list(SHIFT_PRESETS.keys()).index(st.session_state["shift_mode"])
        if st.session_state["shift_mode"] in SHIFT_PRESETS else 0,
    )
    st.session_state["shift_mode"] = shift_mode

    if shift_mode == "Кастом":
        st.caption("Если конец меньше начала — смена через полночь.")
        sh = st.number_input("Старт: час", 0, 23, NIGHT_START[0])
        sm = st.number_input("Старт: мин", 0, 59, NIGHT_START[1])
        eh = st.number_input("Конец: час", 0, 23, NIGHT_END[0])
        em = st.number_input("Конец: мин", 0, 59, NIGHT_END[1])
        SHIFT_PRESETS["Кастом"] = ShiftSpec("Кастом", (int(sh), int(sm)), (int(eh), int(em)), None)

    shift = SHIFT_PRESETS[shift_mode]

    st.divider()
    st.header("3) Правила")
    treat_conn_as_accepted = st.checkbox("Считать CONN как 'Принято'", value=False)
    only_watched_for_missed = st.checkbox("Пропущено считать только по выбранным тематикам", value=shift_mode.startswith("Ночная"))
    watched_skills = st.multiselect("Тематики (skills) для пропущенных", options=sorted(set(WATCHED_SKILLS_DEFAULT)), default=WATCHED_SKILLS_DEFAULT)

    st.divider()
    st.header("4) Хвосты")
    enable_leftovers = st.checkbox("Исключать операторов, кто был только в начале смены", value=shift_mode.startswith("Ночная"))
    leftover_min = st.number_input("Окно начала (мин)", 10, 240, DEFAULT_LEFTOVER_MIN, step=5)

    st.divider()
    st.header("5) Наглядность")
    top_n = st.slider("Топ N", 3, 30, 10)
    max_pivot_cols = st.slider("Макс. операторов в матрице", 3, 50, 15)
    show_raw = st.checkbox("Показывать сырые события", value=False)

if html_file is None:
    st.info("Загрузи HTML Avaya слева. Потом можно переключать ночь/день кнопками.")
    st.stop()

# --- собираем итоговый маппинг ---
mapping: Dict[str, str] = dict(DEFAULT_AGENT_MAP)

uploaded_map = load_agent_mapping(mapping_file)
mapping.update(uploaded_map)

# Вставка текста из реестра
st.markdown("### Операторы (код → имя)")
with st.expander("Добавить операторов вставкой текста из реестра (парсим только строки 'Оператор - ... (код)')", expanded=False):
    registry_text = st.text_area("Вставь сюда текст из реестра (можно вместе с 'Телефон - ...' — он игнорируется)", height=180)
    parsed_map = parse_registry_text_to_mapping(registry_text)
    if parsed_map:
        st.success(f"Найдено операторов: {len(parsed_map)} (будут добавлены/перезаписаны по коду)")
        mapping.update(parsed_map)
    st.caption("Чтобы новые операторы сохранялись в проекте, скачай CSV ниже и храни его рядом — потом просто загружай в приложение.")

# Таблица-редактор
map_df = pd.DataFrame(sorted(mapping.items(), key=lambda x: x[0]), columns=["agent_code", "agent_name"])
edited_df = st.data_editor(
    map_df,
    use_container_width=True,
    num_rows="dynamic",
    key="agent_map_editor",
)

# Пересобираем mapping из отредактированной таблицы
mapping = {}
for _, r in edited_df.fillna("").iterrows():
    code = str(r.get("agent_code", "")).strip()
    name = str(r.get("agent_name", "")).strip()
    if code and name:
        mapping[code] = name

st.download_button(
    "Скачать текущий маппинг операторов (CSV)",
    data=pd.DataFrame(sorted(mapping.items(), key=lambda x: x[0]), columns=["agent_code", "agent_name"]).to_csv(index=False).encode("utf-8"),
    file_name="agent_mapping.csv",
    mime="text/csv",
)

# --- читаем и парсим HTML ---
df_raw = parse_avaya_html_to_df(html_file.getvalue())
df = parse_datetime_columns(df_raw)
if df.empty:
    st.error("Не смог распознать HTML (нужны колонки: Дата, Время нач., Размещение).")
    st.stop()

df = apply_agent_names(df, mapping)

# дата смены
dates = available_shift_dates(df, shift)
date_options = ["Все смены в файле"] + [d.strftime("%d.%m.%Y") for d in dates]
chosen = st.selectbox("Дата смены (если в HTML несколько дней):", options=date_options, index=0)
base_date = None if chosen == "Все смены в файле" else datetime.strptime(chosen, "%d.%m.%Y").date()

df_shift, window = filter_by_shift(df, shift, base_date)
if df_shift.empty:
    st.warning("В выбранной смене нет событий. Попробуй другую дату/режим.")
    st.stop()

excluded: List[str] = []
if enable_leftovers and window is not None:
    df_shift, excluded = drop_leftovers(df_shift, window, int(leftover_min))

metrics = compute_metrics(
    df_shift=df_shift,
    watched_skills=[str(x) for x in watched_skills],
    only_watched_for_missed=only_watched_for_missed,
    treat_conn_as_accepted=treat_conn_as_accepted,
)

# Заголовок окна
if window is not None:
    st_dt, en_dt = window
    st.subheader(f"Окно анализа: {st_dt:%d.%m.%Y %H:%M} → {en_dt:%d.%m.%Y %H:%M} ({shift_mode})")
else:
    st.subheader(f"Окно анализа: {shift_mode}")

if excluded:
    st.caption("Исключены «хвосты»: " + ", ".join(excluded[:20]) + (" ..." if len(excluded) > 20 else ""))

# KPI
kpi = metrics["kpi"].iloc[0].to_dict()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Принято", int(kpi["Принято"]))
c2.metric("Пропущено", int(kpi["Пропущено"]))
c3.metric("Без оператора", int(kpi["Пропущено без оператора"]))
c4.metric("% пропущенных", f'{kpi["% пропущенных"]}%')

# Динамика
st.markdown("### Динамика (получасы)")
ts = metrics["timeseries"].set_index("slot")[["Принято", "Пропущено", "Пропущено_без_оператора"]]
st.line_chart(ts)

# Топы
st.markdown("### Топы")
ops = metrics["operators"]
skills = metrics["skill_summary"]

colA, colB = st.columns(2)
with colA:
    st.markdown(f"**Топ-{top_n} операторов по пропущенным**")
    st.dataframe(ops.head(top_n), use_container_width=True)
with colB:
    st.markdown(f"**Топ-{top_n} тематик по пропущенным**")
    st.dataframe(skills.head(top_n), use_container_width=True)

# Матрица
st.markdown("### Матрица (получас × оператор) — по выбору")
ops_list = ops["agent_name"].tolist() if not ops.empty else []
default_ops = ops_list[: min(len(ops_list), max_pivot_cols)]
selected_ops = st.multiselect("Показать операторов:", options=ops_list, default=default_ops)

if selected_ops:
    piv_acc = metrics["pivot_accepted"]
    piv_mis = metrics["pivot_missed"]
    cc1, cc2 = st.columns(2)
    with cc1:
        st.markdown("**Принятые**")
        st.dataframe(piv_acc[selected_ops] if not piv_acc.empty else pd.DataFrame(), use_container_width=True)
    with cc2:
        st.markdown("**Пропущенные**")
        st.dataframe(piv_mis[selected_ops] if not piv_mis.empty else pd.DataFrame(), use_container_width=True)

# Аномалии
st.markdown("### Аномалии")
st.dataframe(metrics["bad_slots"], use_container_width=True)

# Экспорт
st.markdown("### Экспорт")
events = metrics["events"]
st.download_button(
    "Скачать CSV (события выбранной смены)",
    data=events.to_csv(index=False).encode("utf-8"),
    file_name="avaya_shift_events.csv",
    mime="text/csv",
)

if show_raw:
    st.markdown("### Сырые события (первые 2000 строк)")
    st.dataframe(events.head(2000), use_container_width=True)
