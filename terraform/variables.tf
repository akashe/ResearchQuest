variable "hcloud_token" {
  description = "Hetzner Cloud API token (Project > Security > API Tokens). Pass via TF_VAR_hcloud_token env var — never commit it."
  type        = string
  sensitive   = true
}

variable "ssh_public_key_path" {
  description = "Path to your local SSH public key, used for VM access"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "ssh_allowed_cidrs" {
  description = "CIDR ranges allowed to SSH in. Defaults to open (0.0.0.0/0) — narrow this to your own IP/32 for better security once you know it."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}

variable "server_name" {
  type    = string
  default = "researchquest-neo4j"
}

variable "server_type" {
  description = "Hetzner server type. cpx31/cpx21 (old generation, cheaper) are no longer orderable as of this writing — cpx3x's replacement (cpx32, 8GB) now runs ~EUR42/mo, over budget. cpx22 (2 vCPU / 4GB RAM, ~EUR23/mo) is the affordable option that's actually in stock; docker-compose.yml's memory settings are sized to match this, not the original 8GB target."
  type        = string
  default     = "cpx22"
}

variable "location" {
  description = "Hetzner datacenter: nbg1/fsn1/hel1 = EU, ash = US East, hil = US West"
  type        = string
  default     = "nbg1"
}
