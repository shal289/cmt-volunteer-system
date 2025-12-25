import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = "volunteer_data.db"

@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def query_mentors(
    min_confidence: float,
    recency_days: int | None,
    required_skills: list[str] | None
):
    conn = get_connection()

    query = """
    SELECT
        m.member_id,
        m.member_name,
        m.last_active_date,
        mp.persona_type,
        mp.confidence_score,
        GROUP_CONCAT(s.skill_name, ', ') AS skills
    FROM members m
    JOIN member_personas mp
        ON m.member_id = mp.member_id
    LEFT JOIN member_skills ms
        ON m.member_id = ms.member_id
    LEFT JOIN skills s
        ON ms.skill_id = s.skill_id
    WHERE mp.persona_type = 'Mentor Material'
      AND mp.is_current = 1
      AND mp.confidence_score >= ?
    GROUP BY m.member_id
    """

    df = pd.read_sql_query(query, conn, params=(min_confidence,))

    if df.empty:
        return df


    df["skills"] = df["skills"].fillna("")
    # Ensure datetime is naive to match datetime.now()
    df["last_active_date"] = pd.to_datetime(df["last_active_date"], errors="coerce").dt.tz_localize(None)


    if recency_days is not None:
        cutoff = datetime.now() - pd.Timedelta(days=recency_days)
        df = df[df["last_active_date"] >= cutoff]


    if required_skills and not df.empty:
        required_skills = [s.lower() for s in required_skills]
        df = df[
            df["skills"]
            .str.lower()
            .apply(lambda x: all(skill in x for skill in required_skills))
        ]

    # If filters cleared out the list, return early
    if df.empty:
        return df


    def ranking_score(row):
        confidence = row["confidence_score"]

        if pd.isna(row["last_active_date"]):
            recency_factor = 0.5
        else:
            # Use naive now for comparison
            days_since = (datetime.now() - row["last_active_date"]).days
            recency_factor = max(0.1, 1 - (days_since / 365))

        skill_count = len(row["skills"].split(",")) if row["skills"] else 0
        skill_factor = min(1.0, 0.5 + (0.1 * skill_count))

        return float(confidence * recency_factor * skill_factor)

    # Apply ranking and sort
    df["ranking_score"] = df.apply(ranking_score, axis=1)
    df = df.sort_values("ranking_score", ascending=False)

    return df


st.set_page_config(page_title="CMT Volunteer Mentor Finder", layout="wide")

st.title("📊 CMT Volunteer Mentor Finder")
st.caption("Ranked by confidence, recent activity, and skill relevance")


st.sidebar.header("🔍 Filters")

min_confidence = st.sidebar.slider(
    "Minimum Mentor Confidence",
    min_value=0.0,
    max_value=1.0,
    value=0.6,
    step=0.05
)

recency_days = st.sidebar.selectbox(
    "Active within last (days)",
    options=[None, 30, 60, 90, 180],
    index=0,
    format_func=lambda x: "Any time" if x is None else f"{x} days"
)

skills_input = st.sidebar.text_input(
    "Required Skills (comma-separated)",
    placeholder="python, mentoring, finance"
)

required_skills = (
    [s.strip() for s in skills_input.split(",") if s.strip()]
    if skills_input
    else None
)


st.subheader("🏅 Ranked Mentor Results")

results_df = query_mentors(
    min_confidence=min_confidence,
    recency_days=recency_days,
    required_skills=required_skills
)

if results_df.empty:
    st.info("No mentors found for the selected criteria.")
else:
    # Format the date for cleaner display
    display_df = results_df.copy()
    display_df["last_active_date"] = display_df["last_active_date"].dt.strftime('%Y-%m-%d')
    
    st.dataframe(
        display_df[
            [
                "member_name",
                "confidence_score",
                "ranking_score",
                "last_active_date",
                "skills"
            ]
        ],
        use_container_width=True
    )

st.markdown("---")


st.subheader("⚠️ Low Confidence Members (Review Needed)")

conn = get_connection()
low_conf_df = pd.read_sql_query(
    """
    SELECT
        m.member_name,
        mp.persona_type,
        mp.confidence_score,
        mp.reasoning
    FROM members m
    JOIN member_personas mp
        ON m.member_id = mp.member_id
    WHERE mp.is_current = 1
      AND mp.confidence_score < 0.5
    ORDER BY mp.confidence_score ASC
    """,
    conn
)

if low_conf_df.empty:
    st.success("No low-confidence records 🎉")
else:
    st.dataframe(low_conf_df, use_container_width=True)