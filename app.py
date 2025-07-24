import streamlit as st
import pandas as pd
from genai import *
from custom_logging import logger
from pprint import pformat
from neo4j_operations import create_topic_subgraph, check_top_papers_from_last_3_years, get_year_wise_distribution, get_state_of_the_art_analysis, load_data_if_missing

st.set_page_config(layout="wide")

# Streamlit UI
st.title("Citation Graph Explorer")

if "data_loaded" not in st.session_state:
    load_data_if_missing()
    st.session_state.data_loaded = True

if "graph_name" not in st.session_state:
    st.session_state.graph_name = None
    st.session_state.topic = None
    st.session_state.topic_name = None

topic_input = st.text_input("Topic (Lucene syntax):", "test time scaling")
topic_name = st.text_input("Topic name)")

if st.button("Build Subgraph & Compute PageRank"):
    graph_name = f"subgraph_{topic_name.replace(' ', '_')}"
    print(topic_name, topic_input, graph_name)
    create_topic_subgraph(topic_input, topic_name, graph_name)
    st.session_state.graph_name = graph_name
    st.session_state.topic = topic_input
    st.session_state.topic_name = topic_name
    st.success(f"Subgraph '{graph_name}' created.")

if st.session_state.graph_name:
    topic = st.session_state.topic
    graph_name = st.session_state.graph_name

    if st.button("Top 20 Papers from last 3 Years"):
        
        df = pd.DataFrame(check_top_papers_from_last_3_years(topic, no_of_papers=20))
        df_modified = df.drop(columns=["ID", "Abstract"])

        st.table(df_modified)
        st.markdown(summarize_topic_evolution(df, topic_name), unsafe_allow_html=True)

    if st.button("Year-wise Distribution"):
        
        df = pd.DataFrame(get_year_wise_distribution(topic_name))
        st.bar_chart(df.set_index("year")["paperCount"])

    year_cutoff = st.number_input("After Year", 1900, 2100, 2022)
    if st.button("Top Papers After Year"):
        
        df = pd.DataFrame(get_state_of_the_art_analysis(year_cutoff, topic_name, top_papers_each_year=500))
        df_modified = df.drop(columns=["ID", "Abstract"])
        st.table(df_modified)
        st.markdown(summarize_state_of_art(df, year_cutoff, topic_name), unsafe_allow_html=True)
