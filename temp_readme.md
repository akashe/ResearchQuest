Setup neo4j
    create text index
ingest csv of citation data
setup streamlit secrets
setup .env file

## Phase 1: self-hosted Neo4j+GDS on a VM

`docker-compose.yml` runs Neo4j Community + the GDS plugin (PageRank etc. —
free on Community, no AuraDS needed). Validated locally: GDS 2.13.11 loads
and `gds.pageRank.write` is available, matching what `create_topic_subgraph`
in `neo4j_operations.py`/`neo4j_operations_mcp.py` calls.

**VM provisioning is now automated via Terraform** (`terraform/`) — targets
Hetzner, creates the VM (`cpx31` by default), a firewall (only 22 + 7687
open, 7474 stays closed), an SSH key, and cloud-init installs Docker + the
compose plugin on first boot, so the VM is ready for `docker compose up`
immediately with no manual install step. Validated against the real Hetzner
API with `terraform plan` (3 resources, no drift) — not yet applied.

```
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in hcloud_token, or export TF_VAR_hcloud_token
terraform plan    # review
terraform apply   # creates the real, billable VM
terraform output ssh_command
```

Steps on the VM (Hetzner CPX31 / 8GB RAM or similar — halve the memory env
vars in `docker-compose.yml` for a 4GB box):

1. Docker is already installed via cloud-init if you provisioned with Terraform above.
2. Copy `docker-compose.yml` and `.env` to the VM (small files — plain `scp`
   is fine for these). `.env` must have `NEO4J_PASSWORD` set (reuse
   `.env.example` as a template, pick a strong password — this is what gates
   access once it's ever exposed publicly in Phase 2).
3. `docker compose up -d`, wait for the healthcheck: `docker inspect --format='{{.State.Health.Status}}' researchquest-neo4j`
4. Get the CSVs onto the VM via **private Hetzner Object Storage** (S3-compatible,
   same account as the VM — chosen over publishing the data anywhere public,
   since the Semantic Scholar redistribution question below isn't resolved yet.
   This bucket is private-only: it's just moving your own already-fetched data
   between your own machines, not sharing it with anyone else, so it doesn't
   touch that question):
   - No Terraform resource exists for this — Hetzner Object Storage isn't yet
     supported by the `hcloud` provider. Create the bucket manually: Hetzner
     Cloud Console → Object Storage → New Bucket, then generate an S3
     access key/secret under the bucket's settings.
   - Upload once from your laptop (standard S3 tooling, e.g. `aws-cli`):
     ```
     aws s3 --endpoint-url https://<region>.your-objectstorage.com \
       cp data/citation_nodes_full.csv s3://<bucket>/citation_nodes_full.csv
     aws s3 --endpoint-url https://<region>.your-objectstorage.com \
       cp data/citation_edges_full.csv s3://<bucket>/citation_edges_full.csv
     ```
   - Download on the VM, same way, then run
     `load_data_if_missing()` (loads `data/citation_nodes_full.csv` /
     `citation_edges_full.csv` into Neo4j) exactly as you would locally.
   - Bonus: this also means re-provisioning the VM later doesn't depend on
     your laptop being on — just re-download from the bucket.
5. Ports are bound to `127.0.0.1` only — nothing is reachable from outside the
   VM yet, by design. For ad-hoc browsing from your laptop during setup, open
   an SSH tunnel: `ssh -L 7687:localhost:7687 -L 7474:localhost:7474 <user>@<vm-ip>`.
6. Public exposure (bolt+s over TLS, a domain, firewall rules) is deliberately
   deferred to Phase 2, once the Streamlit app actually needs to reach this
   VM from outside.

## Phase 2: public exposure + Streamlit deployment

`infrastructure/` (the empty k8s/terraform/helm scaffolding) has been removed
per the decision to drop Kubernetes — none of it had any actual content.

Streamlit Community Cloud has no fixed egress IPs to allowlist, so bolt has
to be reachable over the open internet — done via TLS, not by trusting an
open unencrypted port. `docker-compose.prod.yml` adds an nginx TCP/TLS proxy
(`nginx/bolt-proxy.conf`) in front of Neo4j: nginx terminates TLS on the
public port 7687, Neo4j itself stays bound to `127.0.0.1` and is only reached
over the internal Docker network — it never touches a public interface.

**Validated locally end-to-end** with a throwaway self-signed cert: brought
the full stack up (`docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`),
confirmed nginx's config is valid (`nginx -t`), and connected with the real
`neo4j` Python driver over `bolt+s` through the proxy — `RETURN 1` round-tripped
correctly. The only thing to swap for production is a real cert.

Steps on the VM:

1. Get a domain (or subdomain) pointing at the VM's IP.
2. Open only ports `22` (SSH) and `7687` (bolt+s) in the firewall — do not
   open `7474`, Neo4j Browser stays SSH-tunnel-only.
3. Get a cert: `certbot certonly --standalone -d <your-domain>` (standalone
   mode briefly binds port 80 for the ACME challenge — fine as a one-off,
   nginx isn't using port 80 here).
4. `CERT_DIR=/etc/letsencrypt/live/<your-domain> docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`
   — certbot's output directory uses exactly `fullchain.pem`/`privkey.pem`,
   which is what `bolt-proxy.conf` expects, no renaming needed.
5. Set up renewal (Let's Encrypt certs expire every 90 days):
   `certbot renew --deploy-hook "docker exec researchquest-bolt-proxy nginx -s reload"`
   as a cron job.
6. In Streamlit Community Cloud's app dashboard → Secrets, paste:
   ```toml
   [neo4j]
   uri = "bolt+s://<your-domain>:7687"
   user = "neo4j"
   password = "<your NEO4J_PASSWORD>"

   GOOGLE_API_KEY = "<your key>"
   GOOGLE_API_MODEL = "gemini-2.5-pro"
   ```
   (This replaces the old k8s secrets.yaml as the place `GOOGLE_API_KEY` lives —
   `genai.py` currently reads it via `os.environ`/`.env`, so for the deployed
   app it needs to come from Streamlit secrets the same way `app.py` already
   reads `st.secrets["neo4j"]` in `neo4j_operations.py`.)
7. Deploy the app pointing at `app.py` as the main file.

## Phase 3: MCP for others (self-host)

Default approach: others run their own Neo4j + MCP server locally, pointed
at their own copy of the graph — zero ongoing cost or abuse surface on your
side, and no dependency on your VM staying up. `mcp_server.py` already needs
nothing Streamlit-specific — it only imports `neo4j_operations_mcp.py`, not
`genai.py`, so a self-hoster needs just the three `NEO4J_*` vars, not a
Google API key.

Steps for someone self-hosting:

1. Clone the repo, run Phase 1's `docker compose up -d` locally (no VM
   needed for personal/local use — the point of Phase 1's compose file is
   that it works identically on a laptop or a VM).
2. `pip install -r requirements-mcp.txt`
3. Copy `.env.example` → `.env`, fill in the three `NEO4J_*` values to match
   whatever password you set in step 1.
4. Load the graph data (see open question below), then
   `python -c "from neo4j_operations_mcp import load_data_if_missing; load_data_if_missing()"`
5. Point their own `.mcp.json` at their own `mcp_server.py`, same shape as
   this repo's `.mcp.json`.

**Blocked, parked deliberately — the graph data itself.** The MCP server is
only useful with `citation_nodes_full.csv` / `citation_edges_full.csv`
(~1GB+) loaded behind it, and there isn't a clean way to get that to a
self-hoster yet:

- **Publishing it publicly (GitHub Release, Hugging Face Dataset, a public
  bucket, anywhere a stranger could download it) is off the table for now.**
  Semantic Scholar's API license explicitly prohibits repackaging/
  redistributing the API data, and the underlying S2 Data carries its own
  CC BY-NC/ODC-BY-style restrictions on top. This is a real ToS restriction,
  not a formality — confirmed by reading their license terms directly.
- The private Hetzner Object Storage bucket set up in Phase 1 does **not**
  change this — it's for moving your own already-licensed data between your
  own machines (laptop → VM), never shared with anyone else. That's a
  different thing from redistribution and stays fine.
- Once the pipeline migrates to OpenAlex (Phase 4, below) — CC0/public
  domain, no such restriction — publishing a derived dataset publicly
  becomes clean. Revisit this then.

Until then, someone wanting to self-host has to run `build_graph/` and fetch
their own data under their own API usage — no redistribution happens because
nothing derived from your calls changes hands. Slower for them, but the only
option that's unambiguously fine right now.