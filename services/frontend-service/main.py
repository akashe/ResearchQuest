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

        if st.button("Get Top Papers"):
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
                else:
                    st.warning("No papers found")

    st.divider()

    # --- Section 2: State of the Art Analysis ---
    with st.expander("🔍 State of the Art Analysis", expanded=True):
        st.subheader("State of the Art Analysis")
        year_cutoff = st.number_input("After Year", 1900, 2100, 2022)
        num_chunks = st.number_input("How many chunks to process?", min_value=1, max_value=20, value=4, step=1)

        if st.button("Generate State of Art Analysis"):
            with st.spinner("Analyzing state of the art..."):
                result = call_api("/analysis/state-of-art", {
                    "topic_name": topic_name,
                    "year_cutoff": year_cutoff,
                    "num_papers": num_chunks * 500
                })
                
                if result and result.get("analysis"):
                    st.markdown("### State of the Art Analysis")
                    st.markdown(result["analysis"], unsafe_allow_html=True)
                    st.info(f"Processed {result.get('papers_processed', 0)} papers in {result.get('chunks_processed', 0)} chunks")
                else:
                    st.error("Failed to generate analysis")

    st.divider()

    # --- Section 3: Custom Question ---
    with st.expander("🤖 Custom Question", expanded=True):
        st.subheader("Ask a Custom Question")
        year_cutoff_q = st.number_input("After Year (for question)", 1900, 2100, 2022, key="q_year")
        user_question = st.text_input("Ask a custom question:")
        num_chunks_q = st.number_input("How many chunks to process? (for question)", min_value=1, max_value=20, value=4, step=1, key="q_chunks")

        if st.button("Ask Question") and user_question:
            with st.spinner("Processing your question..."):
                result = call_api("/question/custom", {
                    "question": user_question,
                    "topic_name": topic_name,
                    "year_cutoff": year_cutoff_q,
                    "num_chunks": num_chunks_q
                })
                
                if result and result.get("answer"):
                    st.markdown("### Answer")
                    st.markdown(result["answer"], unsafe_allow_html=True)
                    st.info(f"Question: {result.get('question')}")
                    st.info(f"Processed {result.get('papers_processed', 0)} papers")
                else:
                    st.error("Failed to answer question")

else:
    st.info("👆 Please create a subgraph first to enable analysis features")