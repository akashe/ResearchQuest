import os
import time
import csv
from neo4j import GraphDatabase
from tqdm import tqdm
from custom_logging import logger

# Neo4j connection details from environment
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Initialize Neo4j Driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), connection_timeout=300)

def run_query(query, params=None):
    """Execute a Neo4j query"""
    with driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]

def run_batch_query(query, rows):
    """Execute a batch query in Neo4j"""
    with driver.session() as session:
        session.write_transaction(lambda tx: tx.run(query, rows=rows))

def check_neo4j_connection():
    """Check if Neo4j is accessible"""
    try:
        with driver.session() as session:
            session.run("RETURN 1")
        return True
    except Exception as e:
        logger.error(f"Neo4j connection failed: {str(e)}")
        return False

def check_data_presence():
    """Check if data already exists in Neo4j"""
    try:
        node_count = run_query("MATCH (n:Paper) RETURN count(n) AS node_count")[0]["node_count"]
        edge_count = run_query("MATCH (:Paper)-[r:CITES]->(:Paper) RETURN count(r) AS edge_count")[0]["edge_count"]
        
        logger.info(f"Current data: {node_count} nodes, {edge_count} edges")
        return node_count > 0 and edge_count > 0
    except Exception as e:
        logger.error(f"Failed to check data presence: {str(e)}")
        return False

def get_database_stats():
    """Get detailed database statistics"""
    try:
        stats = {}
        
        # Node count
        node_result = run_query("MATCH (n:Paper) RETURN count(n) AS node_count")
        stats["node_count"] = node_result[0]["node_count"]
        
        # Edge count  
        edge_result = run_query("MATCH (:Paper)-[r:CITES]->(:Paper) RETURN count(r) AS edge_count")
        stats["edge_count"] = edge_result[0]["edge_count"]
        
        # Year distribution
        year_result = run_query("""
            MATCH (p:Paper) 
            WHERE p.year IS NOT NULL 
            RETURN p.year AS year, count(*) AS count 
            ORDER BY year DESC 
            LIMIT 10
        """)
        stats["year_distribution"] = year_result
        
        # Index information
        index_result = run_query("SHOW INDEXES")
        stats["indexes"] = len(index_result)
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get database stats: {str(e)}")
        return {"error": str(e)}

def create_index_on_paper_id():
    """Create index on Paper.id for faster lookups"""
    query = """
    CREATE INDEX paper_id_index IF NOT EXISTS FOR (p:Paper) ON (p.id);
    """
    try:
        with driver.session() as session:
            session.run(query)
        logger.info("✅ Index on Paper.id created (or already exists)")
    except Exception as e:
        logger.error(f"Failed to create index: {str(e)}")

def remove_duplicate_nodes():
    """Remove duplicate nodes with same ID"""
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
    try:
        with driver.session() as session:
            session.run(query)
        logger.info("✅ Removed duplicate nodes with same id")
    except Exception as e:
        logger.error(f"Failed to remove duplicates: {str(e)}")

def remove_duplicate_edges():
    """Remove duplicate CITES edges"""
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
    try:
        with driver.session() as session:
            session.run(query)
        logger.info("✅ Removed duplicate CITES edges")
    except Exception as e:
        logger.error(f"Failed to remove duplicate edges: {str(e)}")

def load_nodes_in_batches(csv_file_path, batch_size=100):
    """Load nodes from CSV in streaming batches (memory efficient)"""
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
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"Node CSV file not found: {csv_file_path}")
    
    logger.info(f"Loading nodes from {csv_file_path} in streaming mode...")
    
    total_loaded = 0
    batch_count = 0
    
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)  # Stream reader - doesn't load entire file
        batch = []
        
        for row_count, row in enumerate(reader, 1):
            batch.append(row)
            
            # Process batch when it's full
            if len(batch) >= batch_size:
                try:
                    run_batch_query(query, batch)
                    total_loaded += len(batch)
                    batch_count += 1
                    
                    # Log progress every 50 batches (5000 records)
                    if batch_count % 50 == 0:
                        logger.info(f"✅ Loaded {total_loaded} nodes so far...")
                    
                    batch = []  # Clear batch to free memory
                    
                except Exception as e:
                    logger.error(f"Failed to load batch {batch_count}: {str(e)}")
                    # Continue with next batch instead of failing completely
                    batch = []
                    continue
        
        # Process remaining records in final batch
        if batch:
            try:
                run_batch_query(query, batch)
                total_loaded += len(batch)
                logger.info(f"✅ Loaded final batch of {len(batch)} nodes")
            except Exception as e:
                logger.error(f"Failed to load final batch: {str(e)}")
    
    logger.info(f"✅ Node loading completed: {total_loaded} total nodes loaded")

def load_edges_in_batches(csv_file_path, batch_size=500):
    """Load edges from CSV in streaming batches (memory efficient)"""
    query = """
    UNWIND $rows AS row
    MATCH (source:Paper {id: row.source_id})
    MATCH (target:Paper {id: row.target_id})
    CREATE (source)-[:CITES]->(target)
    """
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"Edge CSV file not found: {csv_file_path}")
    
    logger.info(f"Loading edges from {csv_file_path} in streaming mode...")
    
    total_loaded = 0
    batch_count = 0
    skipped_edges = 0
    
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)  # Stream reader - doesn't load entire file
        batch = []
        
        for row_count, row in enumerate(reader, 1):
            batch.append(row)
            
            # Process batch when it's full
            if len(batch) >= batch_size:
                try:
                    run_batch_query(query, batch)
                    total_loaded += len(batch)
                    batch_count += 1
                    
                    # Log progress every 100 batches (50,000 records)
                    if batch_count % 100 == 0:
                        logger.info(f"✅ Loaded {total_loaded} edges so far...")
                    
                    batch = []  # Clear batch to free memory
                    
                except Exception as e:
                    # Edge loading might fail if source/target nodes don't exist
                    logger.warning(f"Batch {batch_count} failed (likely missing nodes): {str(e)}")
                    skipped_edges += len(batch)
                    batch = []
                    continue
        
        # Process remaining records in final batch
        if batch:
            try:
                run_batch_query(query, batch)
                total_loaded += len(batch)
                logger.info(f"✅ Loaded final batch of {len(batch)} edges")
            except Exception as e:
                logger.warning(f"Final batch failed: {str(e)}")
                skipped_edges += len(batch)
    
    logger.info(f"✅ Edge loading completed: {total_loaded} edges loaded, {skipped_edges} skipped")

def create_fulltext_index():
    """Create fulltext index for paper abstracts and labels"""
    try:
        # Check if index exists
        index_check = '''
        SHOW FULLTEXT INDEXES WHERE name = "paperAbstractIndex"
        '''
        check_index_results = run_query(index_check)
        
        if len(check_index_results) == 0:
            logger.info("Creating fulltext index for paper abstracts...")
            create_index_query = '''
            CREATE FULLTEXT INDEX paperAbstractIndex FOR (p:Paper) ON EACH [p.label, p.abstract];
            '''
            run_query(create_index_query)
            logger.info("✅ Created fulltext index for papers")
        else:
            logger.info("Fulltext index already exists")
            
    except Exception as e:
        logger.error(f"Failed to create fulltext index: {str(e)}")

def load_initial_data():
    """Load initial data from CSV files"""
    try:
        # Check if data already exists
        if check_data_presence():
            logger.info("Data already exists in Neo4j")
            return
        
        logger.info("Starting initial data load...")
        
        # Define file paths (assuming data is mounted in container)
        nodes_file = "/app/data/citation_nodes_full.csv"
        edges_file = "/app/data/citation_edges_full.csv"
        
        # Load nodes
        logger.info("Loading nodes...")
        load_nodes_in_batches(nodes_file, batch_size=500)
        time.sleep(2)
        
        # Create index
        logger.info("Creating indexes...")
        create_index_on_paper_id()
        time.sleep(2)
        
        # Remove duplicate nodes
        logger.info("Removing duplicate nodes...")
        remove_duplicate_nodes()
        time.sleep(2)
        
        # Load edges
        logger.info("Loading edges...")
        load_edges_in_batches(edges_file, batch_size=500)
        time.sleep(2)
        
        # Remove duplicate edges
        logger.info("Removing duplicate edges...")
        remove_duplicate_edges()
        time.sleep(2)
        
        # Create fulltext index
        logger.info("Creating fulltext indexes...")
        create_fulltext_index()
        
        logger.info("✅ Initial data load completed successfully")
        
    except Exception as e:
        logger.error(f"Initial data load failed: {str(e)}")
        raise