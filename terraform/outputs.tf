output "server_ip" {
  description = "Public IPv4 address of the VM"
  value       = hcloud_server.researchquest.ipv4_address
}

output "ssh_command" {
  description = "Quick SSH command to reach the VM"
  value       = "ssh root@${hcloud_server.researchquest.ipv4_address}"
}
