import streamlit as st
import pandas as pd
from genai import *
from custom_logging import logger
from pprint import pformat
from neo4j_operations import create_topic_subgraph, check_top_papers_from_last_3_years, get_year_wise_distribution, get_state_of_the_art_analysis, load_data_if_missing
import time

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

with st.expander("🧭 Subgraph Generation Setup", expanded=True):
    st.markdown('<div class="section-header">Step 1: Define Your Research Topic</div>', unsafe_allow_html=True)
    topic_input = st.text_input("Topic (Lucene syntax):", "test time scaling")
    topic_name = st.text_input("Topic name)")

    st.markdown('<div class="section-header">Step 2: Subgraph Mode</div>', unsafe_allow_html=True)
    validate_relationships = st.radio(
        "Subgraph Mode",
        options=[("Strict (only exact topic)", True), ("Relaxed (related nodes allowed)", False)],
        format_func=lambda x: x[0]
    )[1]

    if st.button("Build Subgraph & Compute PageRank"):
        graph_name = f"subgraph_{topic_name.replace(' ', '_')}"
        print(topic_name, topic_input, graph_name)
        create_topic_subgraph(topic_input, topic_name, graph_name, validate_relationships)
        st.session_state.graph_name = graph_name
        st.session_state.topic = topic_input
        st.session_state.topic_name = topic_name
        st.success(f"Subgraph '{graph_name}' created.")

st.divider()

if st.session_state.graph_name:
    st.divider()
    topic = st.session_state.topic
    graph_name = st.session_state.graph_name

    # --- Section 1: Top Papers from Last N Years ---
    with st.expander("📈 Top Papers from Last N Years", expanded=True):
        st.subheader("Top Papers from Last N Years")
        papers_per_year = st.number_input("How many top papers per year?", min_value=1, max_value=50, value=20, step=1)
        from_year = st.number_input("From which year?", min_value=2019, max_value=2025, value=2022, step=1)
        show_evolution = st.radio(
                "Generate topic evolution summary?",
                options=["Yes", "No"],
                index=0,
                horizontal=True
            ) == "Yes"

        if st.button("Top Papers from Last N Years"):
            df = pd.DataFrame(check_top_papers_from_last_3_years(topic_name,no_of_papers=papers_per_year, from_year=from_year))
            df_modified = df.drop(columns=["ID", "Abstract"])
            st.dataframe(df_modified, use_container_width=True)
            if show_evolution:
                st.markdown("#### Topic Evolution Summary")
                st.markdown(summarize_topic_evolution(df, topic_name), unsafe_allow_html=True)

    st.divider()

    # --- Section 2: Year-wise Distribution ---
    with st.expander("📊 Year-wise Distribution", expanded=True):
        st.subheader("Year-wise Distribution")
        if st.button("Year-wise Distribution"):
            
            df = pd.DataFrame(get_year_wise_distribution(topic_name))
            st.bar_chart(df.set_index("year")["paperCount"])

    st.divider()

    # --- Section 3: State of the Art / Custom Question ---
    with st.expander("🔍 State of the Art / Custom Question", expanded=True):
        st.subheader("State of the Art / Custom Question")
        year_cutoff = st.number_input("After Year", 1900, 2100, 2022)
        user_question = st.text_input("Ask a custom question (leave blank to get a summary):")  
        # question = "A lot of R&D based companies are focusing on the capabilities of reasoning models. What are the things they will need or what would be there next ideas?"
        num_chunks = st.number_input("How many chunks to process?", min_value=1, max_value=20, value=4, step=1)

        if st.button("Top Papers After Year"):
            main_df = pd.DataFrame(get_state_of_the_art_analysis(year_cutoff, topic_name, top_papers_each_year=5000))        
            ask_question = bool(user_question.strip())
            chunk_size = 500
            results = []

            table_df = main_df.iloc[0:100]
            table_df_modified = table_df.drop(columns=["ID", "Abstract"])
            st.dataframe(table_df_modified, use_container_width=True)

            for i in range(num_chunks):
                start = i * chunk_size
                end = (i + 1) * chunk_size
                df = main_df.iloc[start:end]
                if df.empty:
                    break
                if ask_question:
                    result = ask_custom_question(user_question, df, year_cutoff, topic_name)
                else:
                    result = summarize_state_of_art(df, year_cutoff, topic_name)
                st.markdown(result, unsafe_allow_html=True)
                results.append(result)
                st.markdown("----------------------------")
                st.markdown("----------------------------")
                time.sleep(30)

            # Show final combined summary/answer
            if num_chunks > 1:
                final_output = "No answer generated"
                if ask_question:
                    final_output = combine_answers(results, user_question, topic_name, year_cutoff)
                    st.markdown("### Final Combined Answer")
                else:
                    final_output = combine_summaries(results, topic_name, year_cutoff)
                    st.markdown("### Final Combined Summary")
                st.markdown(final_output, unsafe_allow_html=True)


