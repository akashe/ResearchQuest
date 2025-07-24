import streamlit as st
import pandas as pd
from neo4j import GraphDatabase
from genai import *
from custom_logging import logger
from pprint import pformat

# Load Neo4j credentials from Streamlit secrets
uri = st.secrets["neo4j"]["uri"]
user = st.secrets["neo4j"]["user"]
password = st.secrets["neo4j"]["password"]

st.set_page_config(layout="wide")

# Initialize Neo4j Driver
driver = GraphDatabase.driver(uri, auth=(user, password))

def run_query(query, params=None):
    with driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]

def create_topic_subgraph(topic, topic_name, graph_name):

    # This assumes that PaperAbstractIndex is already created in Neo4j
    # If not, you can create it using:
    # CREATE FULLTEXT INDEX paperAbstractIndex FOR (p:Paper) ON EACH [p.label, p.abstract];

    # Check if a full text index exists or not. If not, create it
    check_index_query = '''
SHOW FULLTEXT INDEXES WHERE name CONTAINS "paperAbstractIndex"
'''
    check_index_results = run_query(check_index_query)
    logger.info(f"Checking if fulltext index exists: {len(check_index_results)>0}")

    if len(check_index_results) == 0 :
        logger.info("Creating fulltext index for paper abstracts.")
        create_index_query = '''
        CREATE FULLTEXT INDEX paperAbstractIndex FOR (p:Paper) ON EACH [p.label, p.abstract];
        '''
        logger.info(pformat(run_query(create_index_query)))

    graph_name = f"subgraph_{topic_name.replace(' ', '_')}"

    # Check if subgraph exists, delete it and associated properties
    check_subgraph = f'''
CALL gds.graph.exists('{graph_name}')
YIELD exists
RETURN exists;
'''
    exists = run_query(check_subgraph)
    logger.info(f"Checking if subgraph {graph_name} exists: {exists}")
    if exists and exists[0]['exists']:
        logger.info(f"Subgraph {graph_name} already exists. Dropping it.")
        drop_subgraph = f'''
        CALL gds.graph.drop('{graph_name}');
        '''
        logger.info(pformat(run_query(drop_subgraph)))
        logger.info(pformat(run_query(f'''MATCH (n) REMOVE n.{topic_name}, n.pageRank_{topic_name};''')))

    topic = '" OR "'.join([i.strip() for i in topic.split(",")])
    # topic = " OR ".join([i.strip() for i in topic.split(",")])
    topic = f'("{topic}")'  
    print(f"Creating subgraph for topic: {topic_name} with query: {topic}")

    # Tag papers matching the topic
    tag_q = f'''
CALL db.index.fulltext.queryNodes("paperAbstractIndex", '{topic}')
YIELD node
SET node.{topic_name} = true;
'''
    logger.info(pformat(run_query(tag_q)))

    # # Drop existing graph if present
    # try:
    #     # Drop subgraph if it exists
    #     logger.info(f"Dropping existing subgraph: {graph_name}")
    #     logger.info(pformat(run_query(f"CALL gds.graph.drop('{graph_name}');")))
    #     # Drop existing PageRank property
        
    # except:
    #     pass

    # Project subgraph
    proj_q = f'''
CALL gds.graph.project.cypher(
  '{graph_name}',
  'MATCH (n:Paper) WHERE n.{topic_name}  = true RETURN id(n) AS id',
  'MATCH (a:Paper)-[:CITES]->(b:Paper) RETURN id(a) AS source, id(b) AS target',
  {{validateRelationships: false}}
);
'''
    logger.info(pformat(run_query(proj_q)))

    # Write PageRank to node property
    pr_q = f'''
CALL gds.pageRank.stream('{graph_name}')
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS p, score
SET p.pageRank_{topic_name} = score
'''
    logger.info(pformat(run_query(pr_q)))

# Streamlit UI
st.title("Citation Graph Explorer")

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
        q = f"""
MATCH (p:Paper)
WHERE p.{topic_name} = true AND p.pageRank_{topic_name} IS NOT NULL AND p.year >= 2022
WITH p.year AS year, p
ORDER BY p.pageRank_{topic_name} DESC
WITH year, collect(p)[0..20] AS topPapers
UNWIND topPapers AS p
RETURN year, p.label AS title, p.pageRank_{topic_name} AS pageRank, p.citationCount as CitationCount, p.url AS URL, p.abstract AS Abstract, p.id AS ID
ORDER BY year ASC, pageRank DESC;
"""
        data = run_query(q)
        df = pd.DataFrame(data)
        df_modified = df.drop(columns=["ID", "Abstract"])

        st.table(df_modified)
        # st.markdown(summarize_topic_evolution(df, topic_name), unsafe_allow_html=True)

    if st.button("Year-wise Distribution"):
        q = f"""
MATCH (p:Paper)
WHERE p.{topic_name} = true
RETURN p.year AS year, count(*) AS paperCount
ORDER BY year ASC;
"""
        data = run_query(q)
        df = pd.DataFrame(data)
        st.bar_chart(df.set_index("year")["paperCount"])

    year_cutoff = st.number_input("After Year", 1900, 2100, 2022)
    if st.button("Top Papers After Year"):
        q = f"""
MATCH (p:Paper)
WHERE p.{topic_name} = true AND p.year > {year_cutoff} AND p.pageRank_{topic_name} IS NOT NULL
RETURN p.label AS title, p.year AS year, p.citationCount as CitationCount, p.pageRank_{topic_name} AS subgraphPageRank, p.url AS URL, p.abstract AS Abstract, p.id as ID
ORDER BY subgraphPageRank DESC
LIMIT 500;
"""
        data = run_query(q)
        df = pd.DataFrame(data)
        df_modified = df.drop(columns=["ID", "Abstract"])
        st.table(df_modified)
        # st.markdown(summarize_state_of_art(df, year_cutoff, topic_name), unsafe_allow_html=True)
