Setup neo4j
    create text index
ingest csv of citation data
setup streamlit secrets
setup .env file


docker run \
  --publish=7474:7474 --publish=7687:7687 \
  --volume=$HOME/neo4j/data:/data \
  --volume=$HOME/neo4j/import:/import \
  --env NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  --env NEO4J_PLUGINS='["graph-data-science"]' \
  neo4j