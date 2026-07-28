terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.45"
    }
  }
  required_version = ">= 1.5"
}

provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_ssh_key" "researchquest" {
  name       = "researchquest-deploy"
  public_key = file(var.ssh_public_key_path)
}

# SSH, bolt+s, and 80 (Let's Encrypt HTTP-01 challenge only, needed for
# certbot issuance and every renewal — nothing else listens on it).
# 7474 (Neo4j Browser) is intentionally not exposed — admin access is via
# SSH tunnel only (see temp_readme.md Phase 1).
resource "hcloud_firewall" "researchquest" {
  name = "researchquest-fw"

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = var.ssh_allowed_cidrs
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "7687"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "researchquest" {
  name         = var.server_name
  server_type  = var.server_type
  image        = "ubuntu-22.04"
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.researchquest.id]
  firewall_ids = [hcloud_firewall.researchquest.id]
  user_data    = file("${path.module}/cloud-init.yaml")

  labels = {
    project = "researchquest"
  }
}
