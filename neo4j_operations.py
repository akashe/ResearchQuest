import streamlit as st
from neo4j import GraphDatabase
from custom_logging import logger
from pprint import pformat
import csv
from tqdm import tqdm

# Load Neo4j credentials from Streamlit secrets
uri = st.secrets["neo4j"]["uri"]
user = st.secrets["neo4j"]["user"]
password = st.secrets["neo4j"]["password"]


# Initialize Neo4j Driver
driver = GraphDatabase.driver(uri, auth=(user, password))

def run_query(query, params=None):
    with driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]
    
# Helper to execute a query in batch
def run_batch_query(query, rows):
    with driver.session() as session:
        session.write_transaction(lambda tx: tx.run(query, rows=rows))
    

def check_data_presence():
    node_count = run_query("MATCH (n:Paper) RETURN count(n) AS node_count")[0]["node_count"]
    edge_count = run_query("MATCH (:Paper)-[r:CITES]->(:Paper) RETURN count(r) AS edge_count")[0]["edge_count"]

    logger.info(f"Nodes: {node_count}, Edges: {edge_count}")
    return node_count > 0 and edge_count > 0


# Load CSV in chunks and send to Neo4j
def load_nodes_in_batches(csv_file_path, batch_size=500):
    query = """
    UNWIND $rows AS row
    CREATE (:Paper {
        id: row.id,
        label: row.label,
        year: toInteger(row.year),
        citationCount: toInteger(row.citationCount),
        url: row.url,
        pageRank: toFloat(row.pageRank),
        abstract: row.abstract
    })
    """
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        for i in tqdm(range(0, len(reader), batch_size), desc="Loading nodes"):
            batch = reader[i:i+batch_size]
            run_batch_query(query, batch)

def create_index_on_paper_id():
    query = """
    CREATE INDEX paper_id_index IF NOT EXISTS FOR (p:Paper) ON (p.id);
    """
    with driver.session() as session:
        session.run(query)
    print("✅ Index on Paper.id created (or already exists).")


def remove_duplicate_nodes():
    query = """
    MATCH (p:Paper)
    WITH p.id AS pid, p
    ORDER BY id(p)
    WITH pid, collect(p) AS nodes
    WHERE size(nodes) > 1
    UNWIND nodes[1..] AS toDelete
    CALL {
    WITH toDelete
    DETACH DELETE toDelete
    } IN TRANSACTIONS OF 100 ROWS
    """
    with driver.session() as session:
        session.run(query)
    print("✅ Removed duplicate nodes with same id")


def load_edges_in_batches(csv_file_path, batch_size=500):
    query = """
    UNWIND $rows AS row
    MATCH (source:Paper {id: row.source_id})
    MATCH (target:Paper {id: row.target_id})
    CREATE (source)-[:CITES]->(target)
    """
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        for i in tqdm(range(0, len(reader), batch_size), desc="Loading edges"):
            batch = reader[i:i+batch_size]
            run_batch_query(query, batch)

def remove_duplicate_edges():
    query = """
    MATCH (a:Paper)-[r:CITES]->(b:Paper)
    WITH a, b, collect(r) AS rels
    WHERE size(rels) > 1
    UNWIND rels[1..] AS redundant
    CALL {
    WITH redundant
    DELETE redundant
    } IN TRANSACTIONS OF 100 ROWS
    """
    with driver.session() as session:
        session.run(query)
    print("✅ Removed duplicate CITES edges")

def load_data_if_missing():
    if not check_data_presence():
        logger.info("No data found. Importing nodes and edges...")

        load_nodes_in_batches("data/citation_nodes_full.csv", batch_size=500)
        create_index_on_paper_id()
        remove_duplicate_nodes()
        load_edges_in_batches("data/citation_edges_full.csv", batch_size=500)
        remove_duplicate_edges()
        logger.info("Data load complete.")
    else:
        logger.info("Data already exists in Neo4j.")


def create_topic_subgraph(topic, topic_name, graph_name):

    index_check = '''
    SHOW FULLTEXT INDEXES WHERE name = "paperAbstractIndex"
    '''

    check_index_results = run_query(index_check)
    logger.info(f"Checking if fulltext index exists: {len(check_index_results)>0}")

    if len(check_index_results) == 0 :
        logger.info("Creating fulltext index for paper abstracts.")
        create_index_query = '''
        CREATE FULLTEXT INDEX paperAbstractIndex FOR (p:Paper) ON EACH [p.label, p.abstract];
        '''
        logger.info(pformat(run_query(create_index_query)))

    graph_name = f"subgraph_{topic_name.replace(' ', '_')}"

    # Step 2: Check if Graph Already Exists
    exists_q = f'''
    CALL gds.graph.exists("{graph_name}")
    YIELD exists
    RETURN exists
    '''

    exists = run_query(exists_q)
    logger.info(f"Checking if subgraph {graph_name} exists: {exists}")
    if exists and exists[0]['exists']:
        logger.info(f"Subgraph {graph_name} already exists. Dropping it.")
        drop_subgraph = f'''
        CALL gds.graph.drop('{graph_name}');
        '''
        logger.info(pformat(run_query(drop_subgraph)))
        logger.info(pformat(run_query(f'''MATCH (n) REMOVE n.{topic_name}, n.pageRank_{topic_name};''')))

        # logger.info(f"Subgraph '{graph_name}' already exists. Skipping re-creation.")
        # return

    topic = '" OR "'.join([i.strip() for i in topic.split(",")])
    topic = f'("{topic}")'  
    topic = topic.replace('"', '\\"')  # Escape quotes for Cypher query

    logger.info(f"Creating subgraph: {graph_name} for topic: {topic_name} with query: {topic}")

    proj_q = f'''
    CALL gds.graph.project.cypher(
      "{graph_name}",
      "
        CALL db.index.fulltext.queryNodes('paperAbstractIndex', \\'{topic}\\')
        YIELD node RETURN id(node) AS id
      ",
      "
        CALL db.index.fulltext.queryNodes('paperAbstractIndex', \\'{topic}\\')
        YIELD node AS a
        WITH collect(id(a)) AS ids
        MATCH (x:Paper)-[:CITES]->(y:Paper)
        WHERE id(x) IN ids AND id(y) IN ids
        RETURN id(x) AS source, id(y) AS target
      ",
      {{ validateRelationships: false }}
    )
    '''

    logger.info(f"Projecting subgraph... by running Cypher query:{proj_q}\n\n")
    logger.info(pformat(run_query(proj_q)))

    # Step 4: Compute and write PageRank to topic-specific property
    pr_q = f'''
    CALL gds.pageRank.write("{graph_name}", {{
        writeProperty: "pageRank_{topic_name}"
    }});
    '''

    logger.info("Computing PageRank and writing to property...")
    logger.info(pformat(run_query(pr_q)))


def check_top_papers_from_last_3_years(topic_name, no_of_papers=20):
    """
    Check if the top 20 papers from the last 3 years are already computed.
    """
    q = f"""
    MATCH (p:Paper)
    WHERE p.pageRank_{topic_name} IS NOT NULL AND p.year >= 2022
    WITH p.year AS year, p
    ORDER BY p.pageRank_{topic_name} DESC
    WITH year, collect(p)[0..{no_of_papers}] AS topPapers
    UNWIND topPapers AS p
    RETURN year, p.label AS title, p.pageRank_{topic_name} AS pageRank, p.citationCount as CitationCount, p.url AS URL, p.abstract AS Abstract, p.id AS ID
    ORDER BY year ASC, pageRank DESC;
    """
    
    data = run_query(q)
    return data

def get_year_wise_distribution(topic_name):
    """
    Get year-wise distribution of papers for a given topic.
    """
    q = f"""
    MATCH (p:Paper)
    WHERE p.pageRank_{topic_name} IS NOT NULL
    RETURN p.year AS year, count(*) AS paperCount
    ORDER BY year ASC;
    """
    
    data = run_query(q)
    return data


def get_state_of_the_art_analysis(year_cutoff, topic_name, top_papers_each_year=500):
    """
    Get state of the art analysis for papers after a specific year.
    """
    q = f"""
    MATCH (p:Paper)
    WHERE p.year > {year_cutoff} AND p.pageRank_{topic_name} IS NOT NULL
    RETURN p.label AS title, p.year AS year, p.citationCount as CitationCount, p.pageRank_{topic_name} AS subgraphPageRank, p.url AS URL, p.abstract AS Abstract, p.id as ID
    ORDER BY subgraphPageRank DESC
    LIMIT {top_papers_each_year};
    """
    
    data = run_query(q)
    return data