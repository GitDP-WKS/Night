import io
from datetime import datetime, timedelta, time

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from bs4 import BeautifulSoup
from openpyxl.styles import Font
from openpyxl.formatting.rule import ColorScaleRule

# --- КОНСТАНТЫ и ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (без изменений) ---
# (вставьте сюда все константы и функции из предыдущей версии: parse_duration, combine_datetime и т.д.)
# ... (load_calls, drop_day_leftover, build_summary, export_to_excel — как в последней компактной версии)

# --- НОВЫЕ ГРАФИКИ ---
def plot_load_by_slot(summary: dict):
    df = pd.concat([
        summary["accepted_pivot"].sum(axis=1).rename("Принято"),
        summary["missed_pivot"].sum(axis=1).rename("Пропущено")
    ], axis=1).fillna(0).astype(int)
    df.index = df.index.strftime("%H:%M")

    fig = go.Figure()
    fig.add_trace(go.Bar(x=df.index, y=df["Принято"], name="Принято", marker_color="#2ca02c"))
    fig.add_trace(go.Bar(x=df.index, y=df["Пропущено"], name="Пропущено", marker_color="#d62728"))
    fig.update_layout(
        title="Нагрузка по получасам (принятые + пропущенные)",
        xaxis_title="Время",
        yaxis_title="Количество звонков",
        barmode="stack",
        template="simple_white",
        height=500
    )
    return fig

def plot_top_agents(summary: dict):
    df = summary["accepted_by_agent"].head(15)
    fig = px.bar(df, x="total_accepted", y="agent", orientation="h",
                 title="Топ операторов по принятым звонкам",
                 color="total_accepted",
                 color_continuous_scale="Greens")
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=500)
    return fig

def plot_missed_by_topic(summary: dict):
    if summary["missed_topics"].empty:
        return None
    df = summary["missed_topics"].groupby("topic")["missed_calls"].sum().reset_index()
    fig = px.pie(df, values="missed_calls", names="topic",
                 title="Пропущенные звонки по тематикам",
                 color_discrete_sequence=px.colors.sequential.Reds)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

def plot_heatmap_agents(summary: dict):
    df = summary["accepted_pivot"].copy()
    df.index = df.index.strftime("%H:%M")
    df = df[sorted(df.columns)]  # сортировка операторов

    fig = px.imshow(df.values,
                    labels=dict(x="Оператор", y="Время", color="Звонки"),
                    x=df.columns,
                    y=df.index,
                    color_continuous_scale="Greens",
                    aspect="auto")
    fig.update_layout(title="Активность операторов (принятые звонки)", height=600)
    return fig

# --- ОБНОВЛЁННЫЙ main() с графиками ---
def main():
    st.title("📊 Аналитика ночной смены Avaya")

    file = st.file_uploader("Загрузите HTML-отчёт Avaya", type=["html", "htm"])
    if not file:
        st.info("Загрузите файл для анализа.")
        return

    df = load_calls(file.getvalue().decode("cp1251", errors="ignore"))
    if df.empty:
        st.warning("Нет данных за ночную смену.")
        return

    df = drop_day_leftover(df)
    shift_date = df["shift_date"].iloc[0]

    st.header(f"Ночная смена {shift_date:%d.%m.%Y} (18:30 – 06:30)")

    # Имена операторов
    agents = sorted(df["agent_code"].dropna().unique())
    name_map = {}
    cols = st.columns(2)
    for i, code in enumerate(agents):
        with cols[i % 2]:
            name_map[code] = st.text_input(f"Код {code}", value=code, key=f"n{code}")

    summary = build_summary(df, name_map)

    # === ГРАФИКИ ===
    st.subheader("📈 Нагрузка по времени")
    st.plotly_chart(plot_load_by_slot(summary), use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🏆 Топ операторов")
        st.plotly_chart(plot_top_agents(summary), use_container_width=True)
    with col2:
        pie_fig = plot_missed_by_topic(summary)
        if pie_fig:
            st.subheader("❌ Пропуски по тематикам")
            st.plotly_chart(pie_fig, use_container_width=True)
        else:
            st.info("Нет пропущенных звонков по тематикам 1/3/9")

    st.subheader("🌡️ Тепловая карта активности операторов")
    st.plotly_chart(plot_heatmap_agents(summary), use_container_width=True)

    # === ТАБЛИЦЫ ===
    st.subheader("📋 Подробные таблицы")
    tab1, tab2, tab3, tab4 = st.tabs(["Принятые", "Пропущенные", "Тематики", "Проблемные слоты"])

    with tab1:
        st.dataframe(summary["accepted_pivot"].style.background_gradient(cmap="Greens"), use_container_width=True)
    with tab2:
        st.dataframe(summary["missed_pivot"].style.background_gradient(cmap="Reds"), use_container_width=True)
    with tab3:
        st.dataframe(summary["operator_topic_summary"], use_container_width=True)
    with tab4:
        if not summary["slots_only_missed"].empty:
            disp = summary["slots_only_missed"].copy()
            disp["slot_start"] = disp["slot_start"].dt.strftime("%H:%M")
            st.dataframe(disp.style.background_gradient(subset=["total_missed"], cmap="Reds"))
        if not summary["worked_while_other_missed"].empty:
            wm = summary["worked_while_other_missed"].copy()
            wm["slot_start"] = wm["slot_start"].dt.strftime("%H:%M")
            st.dataframe(wm)

    # === ЭКСПОРТ ===
    st.markdown("---")
    st.download_button(
        label="📥 Скачать полный отчёт в Excel (с градиентом)",
        data=export_to_excel(summary, shift_date),
        file_name=f"Ночная_смена_{shift_date:%d.%m.%Y}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

if __name__ == "__main__":
    main()
