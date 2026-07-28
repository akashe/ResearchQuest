# ResearchQuest

A citation-graph explorer for CS/AI research on arXiv: builds a topic-scoped
subgraph on demand, ranks papers within it with PageRank, and synthesizes
"state of the art" summaries — either through a Streamlit UI or directly
inside Claude Code via MCP.

**Live app:** https://research-quest.streamlit.app/

## How it works

```
arXiv metadata → Semantic Scholar citation lookup → prune → Neo4j (GDS)
                                                                 │
                                        ┌────────────────────────┴───────────────────────┐
                                        │                                                  │
                                  Streamlit UI                                       MCP server
                              (app.py, genai.py)                              (mcp_server.py, Claude Code)
```

The graph lives in Neo4j (Community Edition + the Graph Data Science
plugin — free, self-hosted, no AuraDS needed). Given a topic query, the app
projects a subgraph via GDS's Cypher projection, computes PageRank scoped to
that subgraph, and returns the top-ranked papers. Gemini synthesizes a
state-of-the-art summary or answers a custom question over the results.

## Repo layout

- `app.py`, `genai.py`, `neo4j_operations.py` — the Streamlit app
- `mcp_server.py`, `neo4j_operations_mcp.py` — the MCP server (no Streamlit
  dependency; same graph, exposed as tools for Claude Code)
- `build_graph/` — the ingestion pipeline: arXiv metadata → Semantic Scholar
  paper/citation lookup → pruning → graph export (nodes/edges CSVs)
- `docker-compose.yml` / `docker-compose.prod.yml` / `nginx/` — Neo4j+GDS
  deployment and its TLS bolt proxy (see **Deployment** below)
- `terraform/` — provisions the Hetzner VM this runs on
- `llm_sessions/` — detailed operational notes and session history from
  infra work done with Claude Code; more granular than this file

## Local setup

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in GOOGLE_API_KEY, NEO4J_* creds
# .streamlit/secrets.toml needs a matching [neo4j] section
streamlit run app.py
```

Neo4j itself: `docker compose up -d` (see `docker-compose.yml`) runs a local
Neo4j Community + GDS instance. On first run, the app loads
`data/citation_nodes_full.csv` / `citation_edges_full.csv` into it
automatically.

## MCP (Claude Code)

`mcp_server.py` exposes the graph as tools, registered in `.mcp.json`:

`research_topic` (primary entry point), `create_research_subgraph`,
`list_active_topics`, `get_top_papers_per_year`, `get_top_papers_overall`,
`get_year_distribution`, `search_papers`, `search_papers_in_topic`,
`get_cited_by`, `get_cites`, `get_schema`, `run_cypher`.

```bash
pip install -r requirements-mcp.txt
cp .env.example .env   # only the three NEO4J_* vars are needed for this path
```

## Deployment

Runs on a single Hetzner VM (`terraform/` — Terraform provisions it, firewall
included), not Kubernetes — an earlier, more elaborate architecture attempt
was scrapped as disproportionate to the actual scale here (~1.1M nodes, a
handful of concurrent users). Docker Compose runs Neo4j+GDS on the VM.

**TLS, in plain terms:** Streamlit Community Cloud has no fixed outbound IPs
to allowlist, so the only reliable way to let it reach this Neo4j instance is
over an encrypted, certificate-verified connection (`bolt+s`) — an open,
unencrypted port isn't an option. Getting there needed three pieces:

1. **A hostname for the VM.** TLS certificates are issued for hostnames, not
   bare IPs (bare-IP certs exist now but are short-lived and add renewal
   complexity — not worth it here). Buying a domain wasn't necessary:
   [nip.io](https://nip.io) is a free public DNS service where any hostname
   of the form `<ip-with-dashes>.nip.io` automatically resolves to that IP —
   nothing to register, it's just a nameserver that parses the IP back out
   of the hostname text. This VM uses `167-233-171-28.nip.io`.
2. **A real certificate**, issued by Let's Encrypt via `certbot` for that
   hostname (`certbot certonly --standalone`, using port 80 briefly to prove
   control of the hostname). Valid 90 days.
3. **Something to actually terminate TLS on the public port**, since Neo4j
   itself stays bound to `127.0.0.1` and is never exposed directly. That's
   `nginx/bolt-proxy.conf` — nginx here isn't serving web pages, it's used
   in **stream** mode as a plain TCP/TLS proxy: it listens on the public
   port, terminates TLS using the certbot certificate, and forwards the
   decrypted bolt traffic to Neo4j over the private Docker network. Neo4j
   never has to know TLS is involved at all.

**Auto-renewal:** Ubuntu's `certbot` package installs its own systemd timer
that checks twice daily and renews when a cert is within 30 days of
expiring — no cron setup needed. The one extra piece: renewing writes fresh
certificate files to disk, but nginx caches the loaded certificate in memory
and won't notice on its own. A deploy-hook script at
`/etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh` runs
`docker exec researchquest-bolt-proxy nginx -s reload` — certbot executes
every script in that directory automatically right after a successful
renewal, so the proxy always picks up the new cert without anyone watching it.

See `llm_sessions/temp_readme.md` for the full phase-by-phase deployment log,
including a symlink-mounting gotcha with certbot's directory layout that's
worth knowing about if this ever needs rebuilding from scratch.

## Known constraints

- Citation data comes from Semantic Scholar's API, which restricts bulk
  redistribution of derived data — the graph dataset itself isn't published
  anywhere public. Self-hosting means running `build_graph/` under your own
  API usage.
- Very recent papers naturally have few incoming citation edges yet — that's
  a property of citation graphs generally (it takes time for other papers to
  cite something new), not a data quality bug.
