import streamlit as st
import pandas as pd
import requests
import time
from typing import Dict, Any

# Configuration
QUERY_SERVICE_URL = "http://query-service:8000"

st.set_page_config(layout="wide")

# Streamlit UI
st.title("Citation Graph Explorer - Microservices Edition")
st.markdown(
    """
    <style>
    .main-title { font-size: 2.5em; font-weight: bold; }
    .section-header { font-size: 1.3em; font-weight: 600; color: #4F8BF9; }
    </style>
    """,
    unsafe_allow_html=True
)

def call_api(endpoint: str, data: Dict[str, Any] = None, method: str = "POST"):
    """Helper function to call backend APIs"""
    try:
        url = f"{QUERY_SERVICE_URL}{endpoint}"
        if method == "POST":
            response = requests.post(url, json=data, timeout=300)
        else:
            response = requests.get(url, timeout=60)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Connection Error: {str(e)}")
        return None

# Session state management
if "graph_name" not in st.session_state:
    st.session_state.graph_name = None
    st.session_state.topic = None
    st.session_state.topic_name = None


with st.expander("🧭 Subgraph Generation Setup", expanded=True):
    st.markdown('<div class="section-header">Step 1: Define Your Research Topic</div>', unsafe_allow_html=True)
    topic_input = st.text_input("Topic (Lucene syntax):", "test time scaling")
    topic_name = st.text_input("Topic name:", "test_time_scaling")

    st.markdown('<div class="section-header">Step 2: Subgraph Mode</div>', unsafe_allow_html=True)
    validate_relationships = st.radio(
        "Subgraph Mode",
        options=[("Strict (only exact topic)", True), ("Relaxed (related nodes allowed)", False)],
        format_func=lambda x: x[0]
    )[1]

    if st.button("Build Subgraph & Compute PageRank"):
        with st.spinner("Creating subgraph..."):
            result = call_api("/subgraph/create", {
                "topic": topic_input,
                "topic_name": topic_name,
                "validate_relationships": validate_relationships
            })
            
            if result and result.get("status") == "success":
                st.session_state.graph_name = result.get("graph_name")
                st.session_state.topic = topic_input
                st.session_state.topic_name = topic_name
                st.success(f"Subgraph '{result.get('graph_name')}' created successfully!")
            else:
                st.error("Failed to create subgraph")

st.divider()

if st.session_state.graph_name:
    topic_name = st.session_state.topic_name

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
            with st.spinner("Retrieving papers..."):
                result = call_api("/papers/top-recent", {
                    "topic_name": topic_name,
                    "num_papers": papers_per_year,
                    "from_year": from_year
                })
                
                if result and result.get("papers"):
                    df = pd.DataFrame(result["papers"])
                    df_display = df.drop(columns=["ID", "Abstract"], errors='ignore')
                    st.dataframe(df_display, use_container_width=True)
                    st.info(f"Found {result.get('count', 0)} papers")
                    
                    if show_evolution:
                        with st.spinner("Generating topic evolution summary..."):
                            evolution_result = call_api("/analysis/topic-evolution", {
                                "topic_name": topic_name,
                                "num_papers": papers_per_year,
                                "from_year": from_year
                            })
                            
                            if evolution_result and evolution_result.get("summary"):
                                st.markdown("#### Topic Evolution Summary")
                                st.markdown(evolution_result["summary"], unsafe_allow_html=True)
                            else:
                                st.warning("Failed to generate topic evolution summary")
                else:
                    st.warning("No papers found")

    st.divider()

    # --- Section 2: Year-wise Distribution ---
    with st.expander("📊 Year-wise Distribution", expanded=True):
        st.subheader("Year-wise Distribution")
        if st.button("Year-wise Distribution"):
            with st.spinner("Getting year-wise distribution..."):
                result = call_api(f"/papers/year-distribution/{topic_name}", method="GET")
                
                if result and result.get("distribution"):
                    df = pd.DataFrame(result["distribution"])
                    if not df.empty and "year" in df.columns and "paperCount" in df.columns:
                        st.bar_chart(df.set_index("year")["paperCount"])
                        st.info(f"Showing distribution for {topic_name}")
                    else:
                        st.warning("No distribution data available")
                else:
                    st.warning("Failed to get year-wise distribution")

    st.divider()

    # --- Section 3: State of the Art / Custom Question ---
    with st.expander("🔍 State of the Art / Custom Question", expanded=True):
        st.subheader("State of the Art / Custom Question")
        year_cutoff = st.number_input("After Year", 1900, 2100, 2022)
        user_question = st.text_input("Ask a custom question (leave blank to get a summary):")
        num_chunks = st.number_input("How many chunks to process?", min_value=1, max_value=20, value=4, step=1)

        if st.button("Top Papers After Year"):
            ask_question = bool(user_question.strip())
            
            with st.spinner("Processing request..."):
                if ask_question:
                    # Ask custom question - generation service handles chunking
                    result = call_api("/question/custom", {
                        "question": user_question,
                        "topic_name": topic_name,
                        "year_cutoff": year_cutoff,
                        "num_chunks": num_chunks
                    })
                    
                    if result:
                        # Show first 100 papers table
                        if result.get("papers"):
                            table_df = pd.DataFrame(result["papers"][:100])
                            table_df_display = table_df.drop(columns=["ID", "Abstract"], errors='ignore')
                            st.dataframe(table_df_display, use_container_width=True)
                        
                        # Show the answer
                        if result.get("answer"):
                            st.markdown("### Answer")
                            st.markdown(result["answer"], unsafe_allow_html=True)
                            st.info(f"Processed {result.get('papers_processed', 0)} papers in {result.get('chunks_processed', 0)} chunks")
                        else:
                            st.error("No answer generated")
                    else:
                        st.error("Failed to answer question")
                else:
                    # Get state of art analysis - generation service handles chunking
                    result = call_api("/analysis/state-of-art", {
                        "topic_name": topic_name,
                        "year_cutoff": year_cutoff,
                        "num_chunks": num_chunks  # Let backend know how many papers to process
                    })
                    
                    if result:
                        # Show first 100 papers table
                        if result.get("papers"):
                            table_df = pd.DataFrame(result["papers"][:100])
                            table_df_display = table_df.drop(columns=["ID", "Abstract"], errors='ignore')
                            st.dataframe(table_df_display, use_container_width=True)
                        
                        # Show the analysis
                        if result.get("analysis"):
                            st.markdown("### State of the Art Analysis")
                            st.markdown(result["analysis"], unsafe_allow_html=True)
                            st.info(f"Processed {result.get('papers_processed', 0)} papers in {result.get('chunks_processed', 0)} chunks")
                        else:
                            st.error("No analysis generated")
                    else:
                        st.error("Failed to generate analysis")

else:
    st.info("👆 Please create a subgraph first to enable analysis features")