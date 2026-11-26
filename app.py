import re
import streamlit as st
import pandas as pd
import altair as alt
from genai import (
    summarize_topic_evolution,
    extract_key_points_state_of_art,
    synthesize_state_of_art,
    extract_relevant_info_for_question,
    synthesize_answer_from_extracts,
    run_chunked_extraction,
    CHUNK_SIZE,
    SLEEP_SECONDS,
)
from custom_logging import logger
from neo4j_operations import create_topic_subgraph, check_top_papers_from_last_3_years, get_year_wise_distribution, get_state_of_the_art_analysis, load_data_if_missing

st.set_page_config(layout="wide")

# Streamlit UI
st.title("Citation Graph Explorer")
st.markdown(
    """
    <style>
    .main-title { font-size: 2.5em; font-weight: bold; }
    .section-header { font-size: 1.3em; font-weight: 600; color: #4F8BF9; }
    </style>
    """,
    unsafe_allow_html=True
)

if "data_loaded" not in st.session_state:
    logger.info("DONT HAVE muultiple streamlit windows loaded in browser!!!\n\n")
    load_data_if_missing()
    st.session_state.data_loaded = True

if "graph_name" not in st.session_state:
    st.session_state.graph_name = None
    st.session_state.topic = None
    st.session_state.topic_name = None


def _slugify_topic_name(text: str) -> str:
    """Derives a safe internal identifier from free-typed topic text. This is
    the only sanitization point between user input and the unescaped Cypher
    property/graph names built from it in neo4j_operations.py (pageRank_{name},
    subgraph_{name}, etc.), so keep the charset strict."""
    slug = re.sub(r"[^a-z0-9]+", "_", text.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug[:50] or "topic"


# Collapses automatically once a subgraph exists, so the setup form doesn't
# keep pushing the tabs below (the actual point of the app) out of view.
with st.expander("🧭 Subgraph Generation Setup", expanded=not st.session_state.graph_name):
    st.markdown('<div class="section-header">Define Your Research Topic</div>', unsafe_allow_html=True)
    topic_input = st.text_input("Research topic", "test time scaling")
    st.caption("Separate related phrases with commas, e.g. \"reasoning models, chain-of-thought\".")

    topic_name = _slugify_topic_name(topic_input)
    st.caption(f"Internal graph ID: `{topic_name}`")

    if st.button("Build Subgraph & Compute PageRank"):
        graph_name = f"subgraph_{topic_name}"
        create_topic_subgraph(topic_input, topic_name, graph_name, True)
        st.session_state.graph_name = graph_name
        st.session_state.topic = topic_input
        st.session_state.topic_name = topic_name
        st.session_state.just_built = True
        # Rerun immediately so the expander above collapses on this same
        # interaction (its `expanded=` is only read once, at construction —
        # without this the collapse wouldn't visually happen until the next
        # unrelated click). The success message is shown after the rerun,
        # not here — st.success() right before st.rerun() rarely gets a
        # chance to paint before the rerun replaces the frame.
        st.rerun()

if st.session_state.graph_name:
    topic = st.session_state.topic
    topic_name = st.session_state.topic_name
    graph_name = st.session_state.graph_name

    if st.session_state.pop("just_built", False):
        st.success(f"Subgraph '{graph_name}' created — explore it below. 👇")

    st.divider()
    st.markdown(f'<div class="section-header">📊 Exploring: {topic}</div>', unsafe_allow_html=True)

    def _papers_to_analyze_selectbox(key: str) -> int:
        return st.selectbox(
            "How many top papers to analyze?",
            options=[500, 1000, 2000, 3500, 5000],
            index=2,
            format_func=lambda n: f"{n} papers (~{max(0, n // CHUNK_SIZE - 1) * SLEEP_SECONDS}s+)",
            key=key,
        )

    # st.tabs() has no `key` param — it's a pure layout container with no
    # session_state binding, so which tab is "active" is tracked only by the
    # browser's local component state, not Python. That state can reset to
    # the first tab on reruns triggered from elsewhere in the app. Using
    # segmented_control instead — it's a real input widget (has `key`), so
    # Streamlit correctly persists the selection across any rerun.
    #
    # Options are kept as plain text (icons applied only via format_func for
    # display) rather than baking the icon into the value itself — Streamlit
    # parses a leading emoji out as a separate internal "icon" field, and
    # comparing/persisting against icon-embedded strings hit real friction
    # in testing. Plain values sidestep that entirely.
    SECTIONS = ["State of the Art", "Custom Question", "Top Papers", "Year Distribution"]
    SECTION_ICONS = {"State of the Art": "🔍", "Custom Question": "💬", "Top Papers": "📈", "Year Distribution": "📊"}
    active_section = st.segmented_control(
        "Section",
        options=SECTIONS,
        default=SECTIONS[0],
        format_func=lambda s: f"{SECTION_ICONS[s]} {s}",
        key="active_section",
        label_visibility="collapsed",
    )

    # --- Section: State of the Art ---
    if active_section == "State of the Art":
        st.subheader("State of the Art")
        year_cutoff = st.number_input("After Year", 1900, 2100, 2022, key="sota_year_cutoff")
        papers_to_analyze = _papers_to_analyze_selectbox("sota_papers_to_analyze")

        if st.button("Generate State of the Art Summary"):
            main_df = pd.DataFrame(
                get_state_of_the_art_analysis(year_cutoff, topic_name, top_papers_each_year=papers_to_analyze)
            )
            st.dataframe(main_df.iloc[0:100].drop(columns=["ID", "Abstract"]), use_container_width=True)

            results = run_chunked_extraction(
                main_df,
                lambda chunk: extract_key_points_state_of_art(chunk, year_cutoff, topic_name),
            )
            final_output = synthesize_state_of_art(results, topic_name, year_cutoff)
            st.markdown("### Final Summary")
            st.markdown(final_output, unsafe_allow_html=True)

    # --- Section: Custom Question ---
    elif active_section == "Custom Question":
        st.subheader("Custom Question")
        year_cutoff_q = st.number_input("After Year", 1900, 2100, 2022, key="question_year_cutoff")
        user_question = st.text_input("Ask a question about this topic:")
        papers_to_analyze_q = _papers_to_analyze_selectbox("question_papers_to_analyze")

        if st.button("Answer Question"):
            if not user_question.strip():
                st.warning("Type a question above first.")
            else:
                main_df = pd.DataFrame(
                    get_state_of_the_art_analysis(year_cutoff_q, topic_name, top_papers_each_year=papers_to_analyze_q)
                )
                st.dataframe(main_df.iloc[0:100].drop(columns=["ID", "Abstract"]), use_container_width=True)

                results = run_chunked_extraction(
                    main_df,
                    lambda chunk: extract_relevant_info_for_question(user_question, chunk, year_cutoff_q, topic_name),
                )
                final_output = synthesize_answer_from_extracts(results, user_question, topic_name, year_cutoff_q)
                st.markdown("### Final Answer")
                st.markdown(final_output, unsafe_allow_html=True)

    # --- Section: Top Papers from Last N Years ---
    elif active_section == "Top Papers":
        st.subheader("Top Papers from Last N Years")
        papers_per_year = st.number_input("How many top papers per year?", min_value=1, max_value=50, value=20, step=1)
        from_year = st.number_input("From which year?", min_value=2019, max_value=2026, value=2022, step=1)
        show_evolution = st.radio(
                "Generate topic evolution summary?",
                options=["Yes", "No"],
                index=0,
                horizontal=True
            ) == "Yes"

        if st.button("Show Top Papers"):
            df = pd.DataFrame(check_top_papers_from_last_3_years(topic_name, no_of_papers=papers_per_year, from_year=from_year))
            df_modified = df.drop(columns=["ID", "Abstract"])
            st.dataframe(df_modified, use_container_width=True)
            if show_evolution:
                st.markdown("#### Topic Evolution Summary")
                st.markdown(summarize_topic_evolution(df, topic_name), unsafe_allow_html=True)

    # --- Section: Year-wise Distribution ---
    elif active_section == "Year Distribution":
        st.subheader("Year-wise Distribution")
        if st.button("Show Year-wise Distribution"):
            df = pd.DataFrame(get_year_wise_distribution(topic_name))
            # st.bar_chart's default axis config left label rotation up to
            # Vega-Lite's automatic overlap resolution — explicit Altair
            # chart instead, forcing labelAngle=0 so years always render
            # horizontally regardless of how many distinct years there are.
            chart = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X("year:O", title="Year", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("paperCount:Q", title="Paper Count"),
                )
            )
            st.altair_chart(chart, use_container_width=True)
