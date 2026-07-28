"""
Neo4j operations for MCP server (without Streamlit dependencies)
"""

from neo4j import GraphDatabase
from custom_logging import logger
from pprint import pformat
import csv
import time
from tqdm import tqdm
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASSWORD")

print(f"Connecting to Neo4j at {uri} with user {user}")

driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=300)



def run_query(query, params=None):
    with driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]


def run_batch_query(query, rows):
    with driver.session() as session:
        session.execute_write(lambda tx: tx.run(query, rows=rows).consume())


def check_data_presence():
    # Existence checks (LIMIT 1), not full counts — see neo4j_operations.py's
    # check_data_presence for why (~2.3s -> ~0.3s measured on the real VM).
    has_node = len(run_query("MATCH (n:Paper) RETURN n LIMIT 1")) > 0
    has_edge = len(run_query("MATCH (:Paper)-[r:CITES]->(:Paper) RETURN r LIMIT 1")) > 0
    logger.info(f"Has nodes: {has_node}, Has edges: {has_edge}")
    return has_node and has_edge


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
        reader = csv.DictReader(f)
        batch = []
        for row in tqdm(reader, desc="Loading nodes"):
            batch.append(row)
            if len(batch) >= batch_size:
                run_batch_query(query, batch)
                batch = []
        if batch:
            run_batch_query(query, batch)


def create_index_on_paper_id():
    query = "CREATE INDEX paper_id_index IF NOT EXISTS FOR (p:Paper) ON (p.id);"
    with driver.session() as session:
        session.run(query)


def remove_duplicate_nodes():
    query = """
    MATCH (p:Paper)
    WITH p.id AS pid, p
    ORDER BY id(p)
    WITH pid, collect(p) AS nodes
    WHERE size(nodes) > 1
    UNWIND nodes[1..] AS toDelete
    CALL { WITH toDelete DETACH DELETE toDelete } IN TRANSACTIONS OF 100 ROWS
    """
    with driver.session() as session:
        session.run(query)


def load_edges_in_batches(csv_file_path, batch_size=500):
    query = """
    UNWIND $rows AS row
    MATCH (source:Paper {id: row.source_id})
    MATCH (target:Paper {id: row.target_id})
    CREATE (source)-[:CITES]->(target)
    """
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []
        for row in tqdm(reader, desc="Loading edges"):
            batch.append(row)
            if len(batch) >= batch_size:
                run_batch_query(query, batch)
                batch = []
        if batch:
            run_batch_query(query, batch)


def remove_duplicate_edges():
    query = """
    MATCH (a:Paper)-[r:CITES]->(b:Paper)
    WITH a, b, collect(r) AS rels
    WHERE size(rels) > 1
    UNWIND rels[1..] AS redundant
    CALL { WITH redundant DELETE redundant } IN TRANSACTIONS OF 100 ROWS
    """
    with driver.session() as session:
        session.run(query)


def load_data_if_missing():
    if not check_data_presence():
        logger.info("No data found. Importing nodes and edges...")
        load_nodes_in_batches("data/citation_nodes_full.csv", batch_size=500)
        time.sleep(5)
        create_index_on_paper_id()
        time.sleep(5)
        remove_duplicate_nodes()
        time.sleep(5)
        load_edges_in_batches("data/citation_edges_full.csv", batch_size=500)
        time.sleep(5)
        remove_duplicate_edges()
        time.sleep(5)
        logger.info("Data load complete.")
    else:
        logger.info("Data already exists in Neo4j.")


def create_topic_subgraph(topic, topic_name, graph_name, validate_relationships):
    index_check = 'SHOW FULLTEXT INDEXES WHERE name = "paperAbstractIndex"'
    check_index_results = run_query(index_check)

    if len(check_index_results) == 0:
        logger.info("Creating fulltext index for paper abstracts.")
        run_query('CREATE FULLTEXT INDEX paperAbstractIndex FOR (p:Paper) ON EACH [p.label, p.abstract];')

    graph_name = f"subgraph_{topic_name.replace(' ', '_')}"

    exists_q = f'CALL gds.graph.exists("{graph_name}") YIELD exists RETURN exists'
    exists = run_query(exists_q)
    if exists and exists[0]['exists']:
        logger.info(f"Subgraph {graph_name} already exists. Dropping it.")
        run_query(f"CALL gds.graph.drop('{graph_name}');")
        run_query(f'MATCH (n) REMOVE n.{topic_name}, n.pageRank_{topic_name};')

    topic_terms = [i.strip() for i in topic.split(",")]
    topic_terms.extend([i.lower() for i in topic_terms])
    lucene_query = '" OR "'.join(topic_terms)
    lucene_query = f'"{lucene_query}"'
    lucene_query = lucene_query.replace('"', '\\"')

    proj_q = f'''
    CALL gds.graph.project.cypher(
      "{graph_name}",
      "
        CALL db.index.fulltext.queryNodes('paperAbstractIndex', \\'{lucene_query}\\')
        YIELD node RETURN id(node) AS id
      ",
      "
        CALL db.index.fulltext.queryNodes('paperAbstractIndex', \\'{lucene_query}\\')
        YIELD node AS a
        WITH collect(id(a)) AS ids
        MATCH (x:Paper)-[:CITES]->(y:Paper)
        WHERE id(x) IN ids AND id(y) IN ids
        RETURN id(x) AS source, id(y) AS target
      ",
      {{ validateRelationships: {str(validate_relationships).lower()} }}
    )
    '''
    logger.info(pformat(run_query(proj_q)))

    pr_q = f'CALL gds.pageRank.write("{graph_name}", {{ writeProperty: "pageRank_{topic_name}" }});'
    logger.info(pformat(run_query(pr_q)))
  
  


def get_top_papers_per_year(topic_name, from_year=2022, papers_per_year=20):
    q = f"""
    MATCH (p:Paper)
    WHERE p.pageRank_{topic_name} IS NOT NULL AND p.year >= {from_year}
    WITH p.year AS year, p
    ORDER BY p.pageRank_{topic_name} DESC
    WITH year, collect(p)[0..{papers_per_year}] AS topPapers
    UNWIND topPapers AS p
    RETURN year, p.label AS title, p.pageRank_{topic_name} AS pageRank,
           p.citationCount AS citationCount, p.url AS url, p.abstract AS abstract, p.id AS id
    ORDER BY year ASC, pageRank DESC;
    """
    return run_query(q)


def get_year_wise_distribution(topic_name):
    q = f"""
    MATCH (p:Paper)
    WHERE p.pageRank_{topic_name} IS NOT NULL
    RETURN p.year AS year, count(*) AS paperCount
    ORDER BY year ASC;
    """
    return run_query(q)


def get_top_papers_overall(topic_name, year_cutoff=2022, limit=100, offset=0):
    q = f"""
    MATCH (p:Paper)
    WHERE p.year > {year_cutoff} AND p.pageRank_{topic_name} IS NOT NULL
    RETURN p.label AS title, p.year AS year, p.citationCount AS citationCount,
           p.pageRank_{topic_name} AS pageRank, p.url AS url, p.abstract AS abstract, p.id AS id
    ORDER BY pageRank DESC
    SKIP {offset} LIMIT {limit};
    """
    return run_query(q)


def search_papers(query, limit=50, offset=0):
    """Full-text search across all papers in the graph."""
    q = """
    CALL db.index.fulltext.queryNodes('paperAbstractIndex', $query)
    YIELD node, score
    RETURN node.label AS title, node.year AS year, node.citationCount AS citationCount,
           node.pageRank AS pageRank, node.url AS url, node.abstract AS abstract,
           node.id AS id, score
    ORDER BY score DESC
    SKIP $offset LIMIT $limit;
    """
    return run_query(q, {"query": query, "offset": offset, "limit": limit})


def search_papers_in_topic(topic_name, keyword, limit=50, offset=0):
    """Full-text keyword search restricted to papers within an existing topic subgraph."""
    q = f"""
    CALL db.index.fulltext.queryNodes('paperAbstractIndex', $keyword)
    YIELD node, score
    WHERE node.pageRank_{topic_name} IS NOT NULL
    RETURN node.label AS title, node.year AS year, node.citationCount AS citationCount,
           node.pageRank_{topic_name} AS pageRank, node.url AS url, node.abstract AS abstract,
           node.id AS id, score
    ORDER BY score DESC
    SKIP $offset LIMIT $limit;
    """
    return run_query(q, {"keyword": keyword, "offset": offset, "limit": limit})


def get_cited_by(paper_id, limit=50, sort_by="pagerank", topic_name=None):
    """
    Get papers that cite the given paper.
    sort_by: 'pagerank' (most influential successors) or 'year' (most recent successors)
    topic_name: if provided, restricts results to papers within that topic subgraph
    """
    if topic_name:
        order_clause = f"p.pageRank_{topic_name} DESC" if sort_by == "pagerank" else "p.year DESC, p.pageRank_{topic_name} DESC"
        where_clause = f"AND p.pageRank_{topic_name} IS NOT NULL"
        pagerank_field = f"p.pageRank_{topic_name} AS pageRank"
    else:
        order_clause = "p.pageRank DESC" if sort_by == "pagerank" else "p.year DESC, p.pageRank DESC"
        where_clause = ""
        pagerank_field = "p.pageRank AS pageRank"

    q = f"""
    MATCH (p:Paper)-[:CITES]->(target:Paper {{id: $paper_id}})
    WHERE true {where_clause}
    RETURN p.label AS title, p.year AS year, p.citationCount AS citationCount,
           {pagerank_field}, p.url AS url, p.abstract AS abstract, p.id AS id
    ORDER BY {order_clause}
    LIMIT {limit};
    """
    return run_query(q, {"paper_id": paper_id})


def get_cites(paper_id):
    """Get all papers cited by the given paper (its references). Naturally bounded."""
    q = """
    MATCH (source:Paper {id: $paper_id})-[:CITES]->(p:Paper)
    RETURN p.label AS title, p.year AS year, p.citationCount AS citationCount,
           p.pageRank AS pageRank, p.url AS url, p.abstract AS abstract, p.id AS id
    ORDER BY p.pageRank DESC;
    """
    return run_query(q, {"paper_id": paper_id})


def get_all_topic_names() -> list[str]:
    """Return all topic names that have a computed PageRank property."""
    results = run_query("""
        MATCH (p:Paper)
        WITH keys(p) AS props
        UNWIND props AS prop
        WITH prop WHERE prop STARTS WITH 'pageRank_'
        RETURN DISTINCT replace(prop, 'pageRank_', '') AS topic_name
    """)
    return [r["topic_name"] for r in results]


def get_schema() -> dict:
    """
    Introspect the Neo4j graph and return schema: node properties, relationship types,
    indexes, and all active topic PageRank properties.
    """
    node_props = run_query("""
        CALL db.schema.nodeTypeProperties()
        YIELD nodeLabels, propertyName, propertyTypes
        RETURN nodeLabels, propertyName, propertyTypes
        ORDER BY nodeLabels, propertyName
    """)

    rel_props = run_query("""
        CALL db.schema.relTypeProperties()
        YIELD relType, propertyName, propertyTypes
        RETURN relType, propertyName, propertyTypes
        ORDER BY relType
    """)

    indexes = run_query("""
        SHOW INDEXES
        YIELD name, type, labelsOrTypes, properties, state
        WHERE state = 'ONLINE'
        RETURN name, type, labelsOrTypes, properties
        ORDER BY name
    """)

    topic_props = run_query("""
        MATCH (p:Paper)
        WITH keys(p) AS props
        UNWIND props AS prop
        WITH prop WHERE prop STARTS WITH 'pageRank_'
        RETURN DISTINCT prop AS property
        ORDER BY prop
    """)

    return {
        "node_properties": node_props,
        "relationship_properties": rel_props,
        "indexes": indexes,
        "topic_pagerank_properties": [r["property"] for r in topic_props],
    }


READ_ONLY_PREFIXES = ("match", "call", "return", "with", "unwind", "show", "yield", "where", "optional")
WRITE_KEYWORDS = ("create", "merge", "set", "delete", "detach", "remove", "drop", "load csv")

def _is_read_only(query: str) -> bool:
    """Reject queries containing write clauses."""
    lowered = query.lower()
    for kw in WRITE_KEYWORDS:
        if kw in lowered:
            return False
    return True


def run_cypher(query: str, params: dict | None = None) -> list[dict]:
    """Execute a read-only Cypher query using a read-mode session."""
    if not _is_read_only(query):
        raise ValueError(
            "Write operations are not allowed. Query contains a write clause "
            f"({', '.join(kw for kw in WRITE_KEYWORDS if kw in query.lower())}). "
            "Use read-only MATCH/CALL/RETURN queries only."
        )
    with driver.session() as session:
        result = session.execute_read(lambda tx: list(tx.run(query, params or {})))
        return [record.data() for record in result]


def find_similar_topic(candidate_name: str, search_terms: list[str]) -> str | None:
    """
    Check if any existing topic is semantically similar to the candidate.
    Matches on word overlap between existing topic names and the candidate + search terms.
    Returns the matching topic_name or None.
    """
    existing = get_all_topic_names()
    if not existing:
        return None

    # Build a set of significant words from candidate + search terms
    all_words = set()
    for term in [candidate_name] + search_terms:
        all_words.update(w.lower() for w in term.replace("_", " ").split() if len(w) > 2)

    for topic in existing:
        topic_words = set(w.lower() for w in topic.replace("_", " ").split() if len(w) > 2)
        if topic_words & all_words:  # any word overlap
            return topic

    return None
