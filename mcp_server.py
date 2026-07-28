#!/usr/bin/env python3
"""
ResearchQuest MCP Server
Pure data layer — exposes Neo4j citation graph to Claude for analysis.
"""

import asyncio
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
import mcp.types as types
from dotenv import load_dotenv

from neo4j_operations_mcp import (
    load_data_if_missing,
    run_query,
    create_topic_subgraph,
    get_top_papers_per_year,
    get_year_wise_distribution,
    get_top_papers_overall,
    search_papers,
    search_papers_in_topic,
    get_cited_by,
    get_cites,
    find_similar_topic,
    get_schema,
    run_cypher,
)

load_dotenv()

server = Server("researchquest")


def _format_papers(papers: list[dict]) -> str:
    """Format a list of paper dicts into readable markdown."""
    if not papers:
        return "No papers found."
    lines = []
    for i, p in enumerate(papers, 1):
        lines.append(f"**{i}. {p.get('title', 'Unknown')}** ({p.get('year', '?')})")
        lines.append(f"   - PageRank: {p.get('pageRank', 0):.6f} | Citations: {p.get('citationCount', 0)}")
        lines.append(f"   - ID: {p.get('id', '')}")
        lines.append(f"   - URL: {p.get('url', '')}")
        abstract = p.get('abstract', '') or ''
        lines.append(f"   - Abstract: {abstract[:300]}{'...' if len(abstract) > 300 else ''}")
        lines.append("")
    return "\n".join(lines)


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="create_research_subgraph",
            description=(
                "Build a topic-specific subgraph from the citation network and compute PageRank for it. "
                "Must be called before using get_top_papers_per_year, get_top_papers_overall, "
                "search_papers_in_topic, or get_cited_by with a topic scope. "
                "Use Lucene syntax for topic_query (e.g. 'agentic memory', 'graph RAG', 'reasoning OR inference')."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic_query": {
                        "type": "string",
                        "description": "Lucene search query to match papers (e.g. 'test time scaling')"
                    },
                    "topic_name": {
                        "type": "string",
                        "description": "Short identifier used to reference this topic in other tools (e.g. 'tts', 'graph_rag'). No spaces."
                    },
                    "strict_mode": {
                        "type": "boolean",
                        "description": "True: only matched papers included. False: related papers via citations allowed.",
                        "default": True
                    }
                },
                "required": ["topic_query", "topic_name"]
            }
        ),
        types.Tool(
            name="list_active_topics",
            description="List all topics that have been built and have PageRank computed. Call this first to know what's available.",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="get_top_papers_per_year",
            description=(
                "Get the top-ranked papers per year for a topic, ordered by topic PageRank within each year. "
                "Best for evolution questions: 'how has X changed over time?', 'what were the landmark papers each year?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic_name": {"type": "string", "description": "Topic identifier from create_research_subgraph"},
                    "from_year": {"type": "integer", "description": "Start year (inclusive)", "default": 2020},
                    "papers_per_year": {"type": "integer", "description": "How many top papers to return per year", "default": 10}
                },
                "required": ["topic_name"]
            }
        ),
        types.Tool(
            name="get_year_distribution",
            description="Get paper count per year for a topic. Useful for understanding research volume and momentum.",
            inputSchema={
                "type": "object",
                "properties": {
                    "topic_name": {"type": "string", "description": "Topic identifier"}
                },
                "required": ["topic_name"]
            }
        ),
        types.Tool(
            name="get_top_papers_overall",
            description=(
                "Get the globally top-ranked papers for a topic after a cutoff year, ordered by topic PageRank. "
                "Supports pagination via offset — call repeatedly with increasing offset if you need more papers. "
                "Best for state-of-art analysis and answering research questions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic_name": {"type": "string", "description": "Topic identifier"},
                    "year_cutoff": {"type": "integer", "description": "Only include papers after this year", "default": 2022},
                    "limit": {"type": "integer", "description": "Number of papers to return", "default": 100},
                    "offset": {"type": "integer", "description": "Skip this many papers (for pagination)", "default": 0}
                },
                "required": ["topic_name"]
            }
        ),
        types.Tool(
            name="search_papers",
            description=(
                "Full-text search across ALL papers in the graph regardless of topic. "
                "Use this for cross-domain questions, finding papers on a specific angle without a pre-built subgraph, "
                "or finding papers that bridge two research areas. Supports Lucene syntax."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Lucene search query (e.g. 'graph traversal enterprise', 'memory AND agent')"},
                    "limit": {"type": "integer", "description": "Number of results", "default": 50},
                    "offset": {"type": "integer", "description": "Pagination offset", "default": 0}
                },
                "required": ["query"]
            }
        ),
        types.Tool(
            name="search_papers_in_topic",
            description=(
                "Full-text keyword search restricted to papers within an existing topic subgraph. "
                "Use this to zoom into a specific angle within a broader topic — e.g. searching 'memory consolidation' "
                "within an 'agentic AI' subgraph. Returns results ranked by relevance score."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic_name": {"type": "string", "description": "Topic identifier to search within"},
                    "keyword": {"type": "string", "description": "Search query (Lucene syntax)"},
                    "limit": {"type": "integer", "description": "Number of results", "default": 50},
                    "offset": {"type": "integer", "description": "Pagination offset", "default": 0}
                },
                "required": ["topic_name", "keyword"]
            }
        ),
        types.Tool(
            name="get_cited_by",
            description=(
                "Get papers that cite a given paper — its research successors. "
                "Use sort_by='pagerank' to find the most influential work that built on this paper. "
                "Use sort_by='year' to find the most recent successors. "
                "Optionally scope to a topic subgraph to reduce cardinality and keep results relevant. "
                "Use this to answer: 'what new research directions came from this paper?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "paper_id": {"type": "string", "description": "Paper ID (from id field in other tool results)"},
                    "limit": {"type": "integer", "description": "Max papers to return (manages cardinality)", "default": 50},
                    "sort_by": {"type": "string", "enum": ["pagerank", "year"], "description": "Rank by influence or recency", "default": "pagerank"},
                    "topic_name": {"type": "string", "description": "Optional: restrict to papers within this topic subgraph"}
                },
                "required": ["paper_id"]
            }
        ),
        types.Tool(
            name="get_cites",
            description=(
                "Get papers cited by a given paper — its references and foundations. "
                "Naturally bounded (papers cite ~30-60 others). "
                "Use this to answer: 'what work did this paper build on?', 'what are the foundations of this approach?'"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "paper_id": {"type": "string", "description": "Paper ID (from id field in other tool results)"}
                },
                "required": ["paper_id"]
            }
        ),
        types.Tool(
            name="research_topic",
            description=(
                "PRIMARY ENTRY POINT for any research question. "
                "Call this first when the user asks about a research topic or question. "
                "You provide the search terms you generate from the question; this tool will: "
                "(1) check if a similar topic subgraph already exists and reuse it, "
                "(2) create a new subgraph if not, "
                "(3) return the top papers for you to analyze. "
                "Use pagination (offset) to fetch more papers if your initial analysis needs more depth."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "topic_name": {
                        "type": "string",
                        "description": "Short snake_case identifier you choose for this topic (e.g. 'agentic_memory', 'graph_rag')"
                    },
                    "search_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of search terms you generate to capture the topic broadly (e.g. ['agentic memory', 'agent memory', 'memory for agents', 'LLM memory']). These are OR'd together in the Lucene query."
                    },
                    "year_cutoff": {
                        "type": "integer",
                        "description": "Only return papers after this year",
                        "default": 2020
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of papers to return for initial analysis",
                        "default": 100
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Pagination offset if you need more papers",
                        "default": 0
                    },
                    "strict_mode": {
                        "type": "boolean",
                        "description": "True: only papers matching the query. False: related papers via citations included.",
                        "default": False
                    }
                },
                "required": ["topic_name", "search_terms"]
            }
        ),
        types.Tool(
            name="get_schema",
            description=(
                "Return the full graph schema: node labels and their properties (with types), "
                "relationship types, active indexes, and all topic-specific PageRank properties. "
                "Always call this before run_cypher so you understand the data model. "
                "Also useful when hosted remotely — gives you the live schema without any prior knowledge."
            ),
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="run_cypher",
            description=(
                "Execute a read-only Cypher query directly against Neo4j. "
                "Use this for ad-hoc questions not covered by other tools: "
                "aggregate stats, cross-property filters, custom ranking, graph metrics. "
                "ALWAYS call get_schema first to know node labels, properties, and indexes. "
                "Only MATCH/CALL/RETURN/WITH/UNWIND/SHOW are allowed — write clauses will be rejected."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Read-only Cypher query (no CREATE/MERGE/SET/DELETE/DROP)"
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional query parameters as key-value pairs",
                        "default": {}
                    }
                },
                "required": ["query"]
            }
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    args = arguments or {}

    if name == "create_research_subgraph":
        topic_query = args["topic_query"]
        topic_name = args["topic_name"]
        strict_mode = args.get("strict_mode", True)

        load_data_if_missing()
        graph_name = f"subgraph_{topic_name.replace(' ', '_')}"
        create_topic_subgraph(topic_query, topic_name, graph_name, strict_mode)

        count = run_query(
            f"MATCH (p:Paper) WHERE p.pageRank_{topic_name} IS NOT NULL RETURN count(p) AS count"
        )[0]["count"]

        return [types.TextContent(type="text", text=(
            f"Subgraph '{graph_name}' created for topic '{topic_name}'.\n"
            f"- Papers matched: {count}\n"
            f"- PageRank property: pageRank_{topic_name}\n"
            f"- Mode: {'Strict' if strict_mode else 'Relaxed'}\n\n"
            f"You can now use '{topic_name}' in other tools."
        ))]

    elif name == "list_active_topics":
        results = run_query("""
            MATCH (p:Paper)
            WITH keys(p) AS props
            UNWIND props AS prop
            WITH prop WHERE prop STARTS WITH 'pageRank_'
            RETURN DISTINCT prop AS property
        """)

        if not results:
            return [types.TextContent(type="text", text="No topics built yet. Use create_research_subgraph first.")]

        lines = ["# Active Topics\n"]
        for row in results:
            topic_name = row["property"].replace("pageRank_", "")
            count = run_query(
                f"MATCH (p:Paper) WHERE p.{row['property']} IS NOT NULL RETURN count(p) AS count"
            )[0]["count"]
            lines.append(f"- **{topic_name}** ({count} papers) — use as topic_name in other tools")

        return [types.TextContent(type="text", text="\n".join(lines))]

    elif name == "get_top_papers_per_year":
        topic_name = args["topic_name"]
        from_year = args.get("from_year", 2020)
        papers_per_year = args.get("papers_per_year", 10)

        papers = get_top_papers_per_year(topic_name, from_year, papers_per_year)
        if not papers:
            return [types.TextContent(type="text", text=f"No papers found for topic '{topic_name}' from {from_year}. Does this topic exist? Try list_active_topics.")]

        # Group by year for readability
        from collections import defaultdict
        by_year = defaultdict(list)
        for p in papers:
            by_year[p["year"]].append(p)

        lines = [f"# Top {papers_per_year} Papers per Year — '{topic_name}' (from {from_year})\n"]
        for year in sorted(by_year.keys()):
            lines.append(f"## {year}")
            lines.append(_format_papers(by_year[year]))

        return [types.TextContent(type="text", text="\n".join(lines))]

    elif name == "get_year_distribution":
        topic_name = args["topic_name"]
        data = get_year_wise_distribution(topic_name)

        if not data:
            return [types.TextContent(type="text", text=f"No data for topic '{topic_name}'.")]

        total = sum(r["paperCount"] for r in data)
        lines = [f"# Year Distribution — '{topic_name}' ({total} papers total)\n"]
        for row in data:
            bar = "█" * min(40, row["paperCount"] // max(1, total // 40))
            lines.append(f"{row['year']}: {bar} {row['paperCount']}")

        return [types.TextContent(type="text", text="\n".join(lines))]

    elif name == "get_top_papers_overall":
        topic_name = args["topic_name"]
        year_cutoff = args.get("year_cutoff", 2022)
        limit = args.get("limit", 100)
        offset = args.get("offset", 0)

        papers = get_top_papers_overall(topic_name, year_cutoff, limit, offset)
        if not papers:
            return [types.TextContent(type="text", text=f"No papers found for topic '{topic_name}' after {year_cutoff} at offset {offset}.")]

        header = (
            f"# Top Papers — '{topic_name}' (after {year_cutoff})\n"
            f"Showing {offset + 1}–{offset + len(papers)} ordered by topic PageRank.\n"
            f"Call again with offset={offset + limit} to get the next page.\n"
        )
        return [types.TextContent(type="text", text=header + "\n" + _format_papers(papers))]

    elif name == "search_papers":
        query = args["query"]
        limit = args.get("limit", 50)
        offset = args.get("offset", 0)

        papers = search_papers(query, limit, offset)
        if not papers:
            return [types.TextContent(type="text", text=f"No results for query '{query}'.")]

        header = f"# Search Results — '{query}'\nShowing {offset + 1}–{offset + len(papers)}, ranked by relevance.\n"
        return [types.TextContent(type="text", text=header + "\n" + _format_papers(papers))]

    elif name == "search_papers_in_topic":
        topic_name = args["topic_name"]
        keyword = args["keyword"]
        limit = args.get("limit", 50)
        offset = args.get("offset", 0)

        papers = search_papers_in_topic(topic_name, keyword, limit, offset)
        if not papers:
            return [types.TextContent(type="text", text=f"No results for '{keyword}' within topic '{topic_name}'.")]

        header = f"# Search in '{topic_name}' — '{keyword}'\nShowing {offset + 1}–{offset + len(papers)}, ranked by relevance.\n"
        return [types.TextContent(type="text", text=header + "\n" + _format_papers(papers))]

    elif name == "get_cited_by":
        paper_id = args["paper_id"]
        limit = args.get("limit", 50)
        sort_by = args.get("sort_by", "pagerank")
        topic_name = args.get("topic_name")

        papers = get_cited_by(paper_id, limit, sort_by, topic_name)
        if not papers:
            return [types.TextContent(type="text", text=f"No citing papers found for ID '{paper_id}'.")]

        scope = f"within topic '{topic_name}'" if topic_name else "across all papers"
        header = (
            f"# Papers Citing '{paper_id}'\n"
            f"Showing top {len(papers)} {scope}, sorted by {sort_by}.\n"
        )
        return [types.TextContent(type="text", text=header + "\n" + _format_papers(papers))]

    elif name == "get_cites":
        paper_id = args["paper_id"]
        papers = get_cites(paper_id)

        if not papers:
            return [types.TextContent(type="text", text=f"No references found for paper ID '{paper_id}'.")]

        header = f"# References of '{paper_id}' ({len(papers)} papers)\n"
        return [types.TextContent(type="text", text=header + "\n" + _format_papers(papers))]

    elif name == "research_topic":
        topic_name = args["topic_name"]
        search_terms = args["search_terms"]
        year_cutoff = args.get("year_cutoff", 2020)
        limit = args.get("limit", 100)
        offset = args.get("offset", 0)
        strict_mode = args.get("strict_mode", False)

        load_data_if_missing()

        # Check if a similar topic already exists
        # TODO fix matched topic
        matched_topic = None
        # matched_topic = find_similar_topic(topic_name, search_terms)

        if matched_topic:
            used_topic = matched_topic
            provenance = f"Reusing existing topic **'{matched_topic}'** (matched '{topic_name}')."
        else:
            # Build Lucene OR query from all search terms
            lucene_query = " OR ".join(f'"{t}"' for t in search_terms)
            graph_name = f"subgraph_{topic_name}"
            create_topic_subgraph(lucene_query, topic_name, graph_name, strict_mode)
            used_topic = topic_name
            provenance = (
                f"Created new subgraph **'{topic_name}'** using query: `{lucene_query}`\n"
                f"Mode: {'Strict' if strict_mode else 'Relaxed'}"
            )

        papers = get_top_papers_overall(used_topic, year_cutoff, limit, offset)

        count = run_query(
            f"MATCH (p:Paper) WHERE p.pageRank_{used_topic} IS NOT NULL RETURN count(p) AS count"
        )[0]["count"]

        if not papers:
            return [types.TextContent(type="text", text=(
                f"{provenance}\n\n"
                f"Total papers in subgraph: {count}\n"
                f"No papers found after {year_cutoff} at offset {offset}."
            ))]

        header = (
            f"{provenance}\n\n"
            f"Total papers in subgraph: {count}\n"
            f"Showing {offset + 1}–{offset + len(papers)} after {year_cutoff}, ordered by topic PageRank.\n"
            f"Call again with offset={offset + limit} to fetch the next page.\n"
        )
        return [types.TextContent(type="text", text=header + "\n" + _format_papers(papers))]

    elif name == "get_schema":
        schema = get_schema()

        lines = ["# Graph Schema\n"]

        lines.append("## Node Properties")
        current_label = None
        for row in schema["node_properties"]:
            label = str(row.get("nodeLabels", ""))
            if label != current_label:
                lines.append(f"\n### {label}")
                current_label = label
            lines.append(f"  - `{row['propertyName']}`: {row.get('propertyTypes', 'unknown')}")

        lines.append("\n## Relationship Types")
        for row in schema["relationship_properties"]:
            prop = row.get("propertyName", "(no properties)")
            lines.append(f"  - `{row['relType']}` — property: {prop}")

        lines.append("\n## Indexes (ONLINE)")
        for row in schema["indexes"]:
            lines.append(f"  - `{row['name']}` ({row['type']}) on {row['labelsOrTypes']} → {row['properties']}")

        lines.append("\n## Topic PageRank Properties (available subgraphs)")
        if schema["topic_pagerank_properties"]:
            for prop in schema["topic_pagerank_properties"]:
                topic = prop.replace("pageRank_", "")
                lines.append(f"  - `{prop}` → use topic_name=**'{topic}'** in other tools")
        else:
            lines.append("  - None yet. Use create_research_subgraph or research_topic first.")

        lines.append(
            "\n## Query Tips\n"
            "- Full-text search index: `paperAbstractIndex` on `Paper.label` and `Paper.abstract`\n"
            "- Use `CALL db.index.fulltext.queryNodes('paperAbstractIndex', 'your query')` for text search\n"
            "- Topic PageRank properties follow the pattern `pageRank_<topic_name>`"
        )

        return [types.TextContent(type="text", text="\n".join(lines))]

    elif name == "run_cypher":
        query = args["query"]
        params = args.get("params", {})

        try:
            results = run_cypher(query, params)
        except ValueError as e:
            return [types.TextContent(type="text", text=f"Query rejected: {e}")]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Query error: {e}")]

        if not results:
            return [types.TextContent(type="text", text="Query returned no results.")]

        # Render as a markdown table
        headers = list(results[0].keys())
        rows = [[str(r.get(h, "")) for h in headers] for r in results]
        col_widths = [max(len(h), max((len(row[i]) for row in rows), default=0)) for i, h in enumerate(headers)]

        header_row = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        sep_row = "-|-".join("-" * w for w in col_widths)
        data_rows = "\n".join(
            " | ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row))
            for row in rows
        )

        table = f"```\n{header_row}\n{sep_row}\n{data_rows}\n```"
        return [types.TextContent(type="text", text=f"# Query Results ({len(results)} rows)\n\n{table}")]

    else:
        raise ValueError(f"Unknown tool: {name}")


WORKFLOWS = {
    "state_of_the_art": {
        "description": "Analyze the current state of research on a topic",
        "template": (
            "The user wants to know the state of the art on: {topic}\n\n"
            "Follow this workflow:\n"
            "1. Call `research_topic` with topic_name and search_terms you generate to cover the topic broadly "
            "(e.g. for 'agentic memory': ['agentic memory', 'agent memory', 'memory for agents', 'LLM memory management']). "
            "This will reuse an existing subgraph or create one if missing, then return the top papers.\n"
            "2. Read the abstracts returned. If the first page (100 papers) is insufficient to form a clear picture, "
            "call `get_top_papers_overall` with increasing offset to fetch the next page.\n"
            "3. Synthesize your analysis directly covering: core ideas, dominant techniques, limitations, "
            "open questions, signs of convergence or saturation, and future directions.\n"
            "Do NOT call any external LLM — you are the analyst."
        ),
        "arguments": [{"name": "topic", "description": "The research topic or question", "required": True}]
    },
    "deep_dive": {
        "description": "Deeply understand a research area: key papers, evolution, current state",
        "template": (
            "The user wants to deeply understand: {topic}\n\n"
            "Follow this workflow:\n"
            "1. Call `research_topic` to create/reuse a subgraph and get top papers overall.\n"
            "2. Call `get_year_distribution` to understand research volume and momentum over time.\n"
            "3. Call `get_top_papers_per_year` to see which papers dominated each year — this reveals evolution.\n"
            "4. If you need to zoom into a specific angle within the topic, call `search_papers_in_topic` "
            "with a targeted keyword.\n"
            "5. Synthesize: (a) how the field started and evolved, (b) current state and dominant paradigms, "
            "(c) trajectory — where is it heading, (d) open problems worth exploring."
        ),
        "arguments": [{"name": "topic", "description": "The research area to explore deeply", "required": True}]
    },
    "paper_lineage": {
        "description": "Trace a paper's influence (what it built on, what came from it)",
        "template": (
            "The user wants to understand the lineage of a paper: {paper_title}\n\n"
            "Follow this workflow:\n"
            "1. If you don't have the paper ID, call `search_papers` with the paper title to find it and get its ID.\n"
            "2. Call `get_cites` with the paper ID to see its foundations — what it built on.\n"
            "3. Call `get_cited_by` with sort_by='pagerank' to find the most influential work that followed it.\n"
            "4. Call `get_cited_by` with sort_by='year' to find the most recent successors.\n"
            "5. If citing papers are too numerous, call `get_cited_by` with a topic_name to scope results "
            "to a relevant research area.\n"
            "6. Synthesize: intellectual ancestry, immediate impact, long-term influence, and active research directions "
            "that trace back to this paper."
        ),
        "arguments": [{"name": "paper_title", "description": "Title or description of the paper", "required": True}]
    },
    "cross_domain": {
        "description": "Find connections between two research areas or answer a bridging question",
        "template": (
            "The user wants to explore the intersection of: {areas}\n\n"
            "Follow this workflow:\n"
            "1. Call `search_papers` with a Lucene AND query combining terms from both areas "
            "(e.g. '\"graph RAG\" AND \"reasoning\"'). This searches across ALL papers without needing a subgraph.\n"
            "2. If results are thin, try OR variants or call `search_papers` separately for each area "
            "and look for authors or papers appearing in both result sets.\n"
            "3. For any bridging paper found, call `get_cites` and `get_cited_by` to understand its context.\n"
            "4. Synthesize: what the bridging papers are, how the two fields influence each other, "
            "and where the most active cross-pollination is happening."
        ),
        "arguments": [{"name": "areas", "description": "The two or more research areas to connect", "required": True}]
    },
    "adhoc_query": {
        "description": "Answer a specific question using a custom Cypher query",
        "template": (
            "The user has a specific question answerable from the graph: {question}\n\n"
            "Follow this workflow:\n"
            "1. Call `get_schema` to understand node labels, properties, indexes, and available topic subgraphs.\n"
            "2. Write a targeted read-only Cypher query (MATCH/CALL/RETURN only — no writes allowed).\n"
            "3. Call `run_cypher` with your query.\n"
            "4. If results are empty or unexpected, revisit the schema and adjust the query.\n"
            "5. Interpret the results and answer the user's question directly.\n\n"
            "Useful patterns:\n"
            "- Full-text search: `CALL db.index.fulltext.queryNodes('paperAbstractIndex', 'your terms')`\n"
            "- Topic-scoped filter: `WHERE p.pageRank_<topic_name> IS NOT NULL`\n"
            "- Aggregation: `RETURN p.year AS year, count(*) AS count ORDER BY year`"
        ),
        "arguments": [{"name": "question", "description": "The specific question to answer", "required": True}]
    },
}


@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    return [
        types.Prompt(
            name=name,
            description=wf["description"],
            arguments=[
                types.PromptArgument(
                    name=arg["name"],
                    description=arg["description"],
                    required=arg["required"]
                )
                for arg in wf["arguments"]
            ]
        )
        for name, wf in WORKFLOWS.items()
    ]


@server.get_prompt()
async def handle_get_prompt(name: str, arguments: dict | None) -> types.GetPromptResult:
    if name not in WORKFLOWS:
        raise ValueError(f"Unknown prompt: {name}. Available: {list(WORKFLOWS.keys())}")

    wf = WORKFLOWS[name]
    args = arguments or {}
    text = wf["template"].format(**{arg["name"]: args.get(arg["name"], f"<{arg['name']}>") for arg in wf["arguments"]})

    return types.GetPromptResult(
        description=wf["description"],
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text=text)
            )
        ]
    )


async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="researchquest",
                server_version="0.2.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
