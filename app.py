import io
from datetime import datetime, timedelta, time
from typing import Optional

import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup

# --- НАСТРОЙКИ НОЧНОЙ СМЕНЫ ---

NIGHT_START_HOUR = 18
NIGHT_START_MINUTE = 30
NIGHT_END_HOUR = 6
NIGHT_END_MINUTE = 30

# Окно "начала смены" для отсека дневных операторов (в минутах от старта ночи)
DAY_LEFTOVER_WINDOW_MIN = 60

# Тематики, которые отслеживаем
WATCHED_SKILLS = {"1", "3", "9"}
SKILL_NAMES = {
    "1": "Надежность",
    "3": "Качество э/э",
    "9": "ЭЗС",
}


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def parse_duration(s: str) -> pd.Timedelta:
    """Парсит строки вида '9:50', ':10', ':00' в Timedelta."""
    if s is None:
        return pd.Timedelta(0)
    s = s.strip()
    if not s or s == ":00":
        return pd.Timedelta(0)
    if ":" not in s:
        try:
            return pd.Timedelta(seconds=int(s))
        except ValueError:
            return pd.Timedelta(0)

    parts = s.split(":")
    if len(parts) != 2:
        return pd.Timedelta(0)

    m_part, s_part = parts
    try:
        minutes = int(m_part) if m_part else 0
        seconds = int(s_part) if s_part else 0
    except ValueError:
        return pd.Timedelta(0)
    return pd.Timedelta(minutes=minutes, seconds=seconds)


def combine_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """'26.11.2025', '18:44:28' -> datetime."""
    try:
        d = datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
        t = datetime.strptime(time_str.strip(), "%H:%M:%S").time()
        return datetime.combine(d, t)
    except Exception:
        return None


def parse_skill_code(skill_str: str) -> Optional[str]:
    """
    Из поля Split/Skill берём только код (первое число до пробела/скобки).
    Например: '1 (Надежность)' -> '1'
    """
    if not skill_str:
        return None
    s = skill_str.strip()
    for sep in [" ", "("]:
        if sep in s:
            s = s.split(sep)[0]
            break
    return s.strip() or None


def is_within_night_shift(dt: datetime) -> bool:
    """
    Проверяем, попадает ли datetime в ночную смену 18:30–06:30.
    Ночная смена "через полночь":
      всё >= 18:30 ИЛИ < 06:30.
    """
    if dt is None:
        return False
    total_minutes = dt.hour * 60 + dt.minute
    start = NIGHT_START_HOUR * 60 + NIGHT_START_MINUTE
    end = NIGHT_END_HOUR * 60 + NIGHT_END_MINUTE
    return (total_minutes >= start) or (total_minutes < end)


def get_shift_date(dt: datetime) -> datetime.date:
    """
    Дата ночной смены:
      - если время >= 18:30 -> смена этой даты
      - если время < 06:30 -> смена предыдущей даты
    Пример: 04.12 03:00 -> смена 03.12.
    """
    total_minutes = dt.hour * 60 + dt.minute
    start = NIGHT_START_HOUR * 60 + NIGHT_START_MINUTE
    if total_minutes >= start:
        return dt.date()
    else:
        return (dt - timedelta(days=1)).date()


def floor_to_half_hour(dt: datetime) -> datetime:
    """Округление вниз до начала получаса (17:05 -> 17:00, 17:45 -> 17:30)."""
    minute = (dt.minute // 30) * 30
    return dt.replace(minute=minute, second=0, microsecond=0)


# --- ПАРСИНГ HTML ОТЧЁТА ---

def load_calls_from_html_text(html_text: str) -> pd.DataFrame:
    soup = BeautifulSoup(html_text, "lxml")
    table = soup.find("table")
    if table is None:
        st.error("В HTML не найдена таблица <table> с данными отчёта.")
        return pd.DataFrame()

    rows = table.find_all("tr")
    if not rows:
        st.error("В таблице нет строк.")
        return pd.DataFrame()

    data_rows = rows[1:]  # пропускаем заголовок
    records = []

    for r in data_rows:
        cells = r.find_all("td")
        if not cells:
            continue

        texts = [c.get_text(strip=True).replace("\xa0", " ") for c in cells]

        # Ожидается минимум 19 колонок по формату Avaya CMS
        if len(texts) < 19:
            continue

        call_id = texts[0]
        segment = texts[1]
        date_str = texts[2]
        time_str = texts[3]
        calling = texts[4]
        dialed = texts[5]
        disposition = texts[6]  # ANS / ABAN / ...
        ring_time = texts[7]
        queue_time = texts[8]
        skill = texts[9]
        agent = texts[10]
        talk_time = texts[11]
        hold_time = texts[12]
        aftercall_time = texts[13]
        external_transfer = texts[14]
        conference = texts[15]
        help_ = texts[16]
        sl_group = texts[17]
        end_code = texts[18]

        dt = combine_datetime(date_str, time_str)
        if dt is None:
            continue
        if not is_within_night_shift(dt):
            # игнорируем вызовы вне ночной смены
            continue

        record = {
            "call_id": call_id,
            "segment": segment,
            "date_str": date_str,
            "time_str": time_str,
            "datetime": dt,
            "shift_date": get_shift_date(dt),
            "calling": calling,
            "dialed": dialed,
            "disposition": disposition,
            "ring_time": parse_duration(ring_time),
            "queue_time": parse_duration(queue_time),
            "skill_raw": skill,
            "skill_code": parse_skill_code(skill),
            "agent_code": agent if agent else None,
            "talk_time": parse_duration(talk_time),
            "hold_time": parse_duration(hold_time),
            "aftercall_time": parse_duration(aftercall_time),
            "external_transfer": external_transfer,
            "conference": conference,
            "help": help_,
            "sl_group": sl_group,
            "end_code": end_code,
        }

        records.append(record)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["slot_start"] = df["datetime"].apply(floor_to_half_hour)
    return df


# --- АНАЛИТИКА ---

def build_slot_range(shift_date: datetime.date) -> pd.DatetimeIndex:
    """Все получасы с 18:30 shift_date до 06:30 следующего дня."""
    start_dt = datetime.combine(shift_date, time(NIGHT_START_HOUR, NIGHT_START_MINUTE))
    end_dt = datetime.combine(shift_date + timedelta(days=1),
                              time(NIGHT_END_HOUR, NIGHT_END_MINUTE))
    # левая граница включительно, правая — нет
    return pd.date_range(start=start_dt, end=end_dt, freq="30min", inclusive="left")


def drop_day_leftover_agents(df: pd.DataFrame) -> pd.DataFrame:
    """
    Убирает операторов, которые фигурируют только в самом начале ночной смены,
    считая, что они просто задержались с дневной смены.
    Правило: если последний вызов оператора был в первые DAY_LEFTOVER_WINDOW_MIN минут
    от начала смены, и дальше его нет, оператор исключается.
    """
    if df.empty:
        return df

    shift_date = df["shift_date"].iloc[0]
    shift_start = datetime.combine(shift_date, time(NIGHT_START_HOUR, NIGHT_START_MINUTE))
    window_end = shift_start + timedelta(minutes=DAY_LEFTOVER_WINDOW_MIN)

    by_agent = (
        df[df["agent_code"].notna()]
        .groupby("agent_code")["datetime"]
        .max()
    )

    leftover_codes = by_agent[by_agent <= window_end].index.tolist()
    if not leftover_codes:
        return df

    return df[~df["agent_code"].isin(leftover_codes)].copy()


def build_summary_tables(df_shift: pd.DataFrame, agent_name_map: dict):
    """Строит сводные таблицы по выбранной смене."""

    df = df_shift.copy()
    # красивые имена
    df["agent"] = df["agent_code"].map(agent_name_map).fillna(df["agent_code"])

    # принятые
    ans = df[(df["disposition"] == "ANS") & df["agent"].notna()].copy()

    # пропущенные с оператором (ABAN, skill 1/3/9)
    missed_by_agent = df[
        (df["disposition"] == "ABAN") &
        (df["skill_code"].isin(WATCHED_SKILLS)) &
        (df["agent_code"].notna())
    ].copy()

    # ABAN без оператора
    missed_no_agent = df[
        (df["disposition"] == "ABAN") &
        (df["skill_code"].isin(WATCHED_SKILLS)) &
        (df["agent_code"].isna())
    ].copy()

    # Названия тематик
    for sub in (ans, missed_by_agent, missed_no_agent):
        if not sub.empty:
            sub["topic"] = sub["skill_code"].map(SKILL_NAMES).fillna("Другое / нет")

    # диапазон получасов
    shift_date = df_shift["shift_date"].iloc[0]
    slots = build_slot_range(shift_date)

    # --- принятые по получасам/операторам ---
    if not ans.empty:
        accepted_counts = (
            ans.groupby(["slot_start", "agent"])
            .agg(calls=("call_id", "count"))
            .reset_index()
        )
        accepted_pivot = (
            accepted_counts
            .pivot(index="slot_start", columns="agent", values="calls")
            .reindex(slots)
            .fillna(0)
            .astype(int)
        )
    else:
        accepted_counts = pd.DataFrame(columns=["slot_start", "agent", "calls"])
        accepted_pivot = pd.DataFrame(index=slots)

    # --- пропущенные по получасам/операторам ---
    if not missed_by_agent.empty:
        missed_counts = (
            missed_by_agent.groupby(["slot_start", "agent"])
            .agg(missed_calls=("call_id", "count"))
            .reset_index()
        )
        missed_pivot = (
            missed_counts
            .pivot(index="slot_start", columns="agent", values="missed_calls")
            .reindex(slots)
            .fillna(0)
            .astype(int)
        )
    else:
        missed_counts = pd.DataFrame(columns=["slot_start", "agent", "missed_calls"])
        missed_pivot = pd.DataFrame(index=slots)

    # --- детализация по тематикам (по получасам) ---
    if not ans.empty:
        accepted_topics = (
            ans.groupby(["slot_start", "agent", "topic"])
            .agg(calls=("call_id", "count"))
            .reset_index()
            .sort_values(["slot_start", "agent", "topic"])
        )
    else:
        accepted_topics = pd.DataFrame(columns=["slot_start", "agent", "topic", "calls"])

    if not missed_by_agent.empty:
        missed_topics = (
            missed_by_agent.groupby(["slot_start", "agent", "topic"])
            .agg(missed_calls=("call_id", "count"))
            .reset_index()
            .sort_values(["slot_start", "agent", "topic"])
        )
    else:
        missed_topics = pd.DataFrame(columns=["slot_start", "agent", "topic", "missed_calls"])

    if not missed_no_agent.empty:
        missed_no_agent_topics = (
            missed_no_agent.groupby(["slot_start", "topic"])
            .agg(missed_calls=("call_id", "count"))
            .reset_index()
            .sort_values(["slot_start", "topic"])
        )
    else:
        missed_no_agent_topics = pd.DataFrame(columns=["slot_start", "topic", "missed_calls"])

    # --- общие принятые по операторам ---
    if not ans.empty:
        accepted_by_agent = (
            ans.groupby("agent")
            .agg(total_accepted=("call_id", "count"))
            .reset_index()
            .sort_values("total_accepted", ascending=False)
        )
    else:
        accepted_by_agent = pd.DataFrame(columns=["agent", "total_accepted"])

    # --- сводка по операторам и тематикам ---
    if not ans.empty:
        acc_topic_agent = (
            ans.groupby(["agent", "topic"])
            .size()
            .unstack(fill_value=0)
        )
    else:
        acc_topic_agent = pd.DataFrame()

    if not missed_by_agent.empty:
        miss_topic_agent = (
            missed_by_agent.groupby(["agent", "topic"])
            .size()
            .unstack(fill_value=0)
        )
    else:
        miss_topic_agent = pd.DataFrame()

    all_agents = sorted(set(acc_topic_agent.index) | set(miss_topic_agent.index))
    all_topics = sorted(set(acc_topic_agent.columns) | set(miss_topic_agent.columns))

    operator_topic_summary = pd.DataFrame(index=all_agents)
    for topic in all_topics:
        acc_col = f"Принято: {topic}"
        miss_col = f"Пропущено: {topic}"
        acc_series = acc_topic_agent[topic] if topic in acc_topic_agent.columns else pd.Series(0, index=acc_topic_agent.index)
        miss_series = miss_topic_agent[topic] if topic in miss_topic_agent.columns else pd.Series(0, index=miss_topic_agent.index)
        operator_topic_summary[acc_col] = acc_series.reindex(all_agents).fillna(0).astype(int)
        operator_topic_summary[miss_col] = miss_series.reindex(all_agents).fillna(0).astype(int)

    if not operator_topic_summary.empty:
        operator_topic_summary = operator_topic_summary.reset_index().rename(columns={"index": "Оператор"})
    else:
        operator_topic_summary = pd.DataFrame(columns=["Оператор"])

    # --- логика: получасовки только с пропущенными, и "один работал, другой пропускал" ---

    # сводка slot_agent: для каждого слота и оператора — calls / missed_calls
    if not accepted_counts.empty:
        slot_agent = accepted_counts[["slot_start", "agent", "calls"]].copy()
    else:
        slot_agent = pd.DataFrame(columns=["slot_start", "agent", "calls"])

    if not missed_counts.empty:
        slot_agent = pd.merge(
            slot_agent,
            missed_counts[["slot_start", "agent", "missed_calls"]],
            on=["slot_start", "agent"],
            how="outer",
        )
    else:
        slot_agent["missed_calls"] = 0

    if not slot_agent.empty:
        slot_agent = slot_agent.fillna(0)
        slot_agent["calls"] = slot_agent["calls"].astype(int)
        slot_agent["missed_calls"] = slot_agent["missed_calls"].astype(int)

    # получасовки, где только пропущенные
    if not slot_agent.empty:
        total_per_slot = (
            slot_agent.groupby("slot_start")
            .agg(
                total_accepted=("calls", "sum"),
                total_missed=("missed_calls", "sum"),
            )
            .reset_index()
        )
        slots_only_missed = total_per_slot[
            (total_per_slot["total_missed"] > 0) &
            (total_per_slot["total_accepted"] == 0)
        ].copy()
    else:
        slots_only_missed = pd.DataFrame(columns=["slot_start", "total_accepted", "total_missed"])

    # "один работал, другой пропускал"
    worked_while_other_missed_records = []
    if not slot_agent.empty:
        for slot in slots:
            sa_slot = slot_agent[slot_agent["slot_start"] == slot]
            if sa_slot.empty:
                continue

            workers = sa_slot[sa_slot["calls"] > 0]["agent"].unique().tolist()
            if not workers:
                continue

            sleepers = sa_slot[
                (sa_slot["missed_calls"] > 0) & (sa_slot["calls"] == 0)
            ]

            for _, row in sleepers.iterrows():
                sleeper = row["agent"]
                missed_c = int(row["missed_calls"])
                for worker in workers:
                    if worker == sleeper:
                        continue
                    worked_while_other_missed_records.append({
                        "slot_start": slot,
                        "worker_agent": worker,
                        "sleeper_agent": sleeper,
                        "missed_calls_by_sleeper": missed_c,
                    })

    worked_while_other_missed = pd.DataFrame(worked_while_other_missed_records)
    if not worked_while_other_missed.empty:
        worked_while_other_missed = worked_while_other_missed.sort_values(
            ["slot_start", "sleeper_agent", "worker_agent"]
        )

    return {
        "accepted_pivot": accepted_pivot,
        "missed_pivot": missed_pivot,
        "accepted_topics": accepted_topics,
        "missed_topics": missed_topics,
        "missed_no_agent_topics": missed_no_agent_topics,
        "slots_only_missed": slots_only_missed,
        "worked_while_other_missed": worked_while_other_missed,
        "accepted_by_agent": accepted_by_agent,
        "operator_topic_summary": operator_topic_summary,
    }


# --- СТИЛИ ---

def style_accepted(df: pd.DataFrame):
    if df.empty:
        return df
    return df.style.applymap(
        lambda v: "background-color: #e6ffed; color: black; font-weight: 600"
        if isinstance(v, (int, float)) and v > 0 else ""
    )


def style_missed(df: pd.DataFrame):
    if df.empty:
        return df
    return df.style.applymap(
        lambda v: "background-color: #ffe6f0; color: black; font-weight: 600"
        if isinstance(v, (int, float)) and v > 0 else ""
    )


def style_slots_only_missed(df: pd.DataFrame):
    if df.empty:
        return df

    def _row_style(_row):
        return ["background-color: #ffe6f0; color: black; font-weight: 600"] * len(_row)

    return df.style.apply(_row_style, axis=1)


def style_worked_while_other_missed(df: pd.DataFrame):
    if df.empty:
        return df
    cols = list(df.columns)

    def _row_style(row):
        styles = [""] * len(cols)
        if "worker_agent" in cols:
            styles[cols.index("worker_agent")] = "background-color: #e6ffed; color: black; font-weight: 600"
        if "sleeper_agent" in cols:
            styles[cols.index("sleeper_agent")] = "background-color: #ffe6f0; color: black; font-weight: 600"
        return styles

    return df.style.apply(_row_style, axis=1)


def style_total_accepted(df: pd.DataFrame):
    if df.empty:
        return df
    return df.style.applymap(
        lambda v: "background-color: #e6ffed; color: black; font-weight: 600"
        if isinstance(v, (int, float)) and v > 0 else ""
    )


def style_operator_topic(df: pd.DataFrame):
    if df.empty:
        return df

    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for c in df.columns:
        if c.startswith("Принято:"):
            styles[c] = [
                "background-color: #e6ffed; color: black; font-weight: 600" if v > 0 else ""
                for v in df[c]
            ]
        elif c.startswith("Пропущено:"):
            styles[c] = [
                "background-color: #ffe6f0; color: black; font-weight: 600" if v > 0 else ""
                for v in df[c]
            ]
    return df.style.apply(lambda _: styles, axis=None)


# --- STREAMLIT UI ---

def main():
    st.title("Аналитика ночной смены: принятые и пропущенные звонки")

    uploaded_file = st.file_uploader("Загрузите HTML-отчёт Avaya", type=["html", "htm"])
    if not uploaded_file:
        st.info("Загрузите файл отчёта, чтобы начать анализ.")
        return

    html_text = uploaded_file.getvalue().decode("cp1251", errors="ignore")
    df_all = load_calls_from_html_text(html_text)
    if df_all.empty:
        st.warning("После отбора по ночной смене (18:30–06:30) записи не найдены.")
        return

    # одна смена в файле
    shift_date = df_all["shift_date"].iloc[0]

    # убираем тех, кто просто задержался с дня
    df_all = drop_day_leftover_agents(df_all)
    df_shift = df_all.copy()

    st.subheader(
        f"Ночная смена: {shift_date.strftime('%d.%m.%Y')} 18:30 "
        f"– {(shift_date + timedelta(days=1)).strftime('%d.%m.%Y')} 06:30"
    )

    # коды операторов
    agent_codes = sorted(df_shift["agent_code"].dropna().unique())
    st.markdown("### Имена операторов")
    st.caption("Можно переименовать коды операторов для удобства (как их зовут в эту смену).")

    agent_name_map = {}
    cols = st.columns(2)
    for i, code in enumerate(agent_codes):
        with cols[i % 2]:
            name = st.text_input(f"Код {code}", value=str(code))
            agent_name_map[code] = name

    summary = build_summary_tables(df_shift, agent_name_map)

    st.markdown("### Принятые звонки по получасам и операторам (ANS)")
    st.caption("Зелёным подсвечены ячейки, где у оператора были принятые звонки в данном получасе.")
    st.dataframe(style_accepted(summary["accepted_pivot"]), use_container_width=True)

    st.markdown("### Пропущенные звонки по получасам и операторам (ABAN с оператором)")
    st.caption(
        "Розовым подсвечены ячейки, где у оператора были пропущенные звонки (ABAN) по тематикам 1/3/9 — "
        "клиент не дождался ответа, хотя оператор был привязан."
    )
    st.dataframe(style_missed(summary["missed_pivot"]), use_container_width=True)

    st.markdown("### Общее количество принятых звонков по операторам")
    st.dataframe(style_total_accepted(summary["accepted_by_agent"]), use_container_width=True)

    st.markdown("### Сводка по операторам и тематикам")
    st.caption("Зелёные ячейки — сколько принято по тематике, розовые — сколько пропущено по тематике.")
    st.dataframe(style_operator_topic(summary["operator_topic_summary"]), use_container_width=True)

    st.markdown("### Детализация принятых звонков по тематикам (по получасам)")
    st.dataframe(summary["accepted_topics"], use_container_width=True)

    st.markdown("### Детализация пропущенных звонков по тематикам (по получасам, с оператором)")
    st.dataframe(summary["missed_topics"], use_container_width=True)

    st.markdown("### Звонки, которые никто не принял (ABAN без оператора, тематики 1/3/9)")
    st.dataframe(summary["missed_no_agent_topics"], use_container_width=True)

    st.markdown("### Получасовки, где были пропущенные звонки и ни одного принятого")
    st.caption("В этих интервалах были пропущенные звонки (ABAN по тематикам 1/3/9), и ни один оператор не принял ни одного звонка.")
    st.dataframe(style_slots_only_missed(summary["slots_only_missed"]), use_container_width=True)

    st.markdown("### Ситуации: один оператор работал, пока другой пропускал звонки")
    st.caption(
        "Зелёным — оператор, который в этом получасе принимал звонки. "
        "Розовым — оператор, у которого в это же время были пропущенные звонки (ABAN) и ни одного принятого."
    )
    st.dataframe(
        style_worked_while_other_missed(summary["worked_while_other_missed"]),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
