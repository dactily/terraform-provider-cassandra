# Terraform Cassandra Provider

This Terraform provider allows you to manage Cassandra (and Scylla) resources such as keyspaces, roles, grants, and tables. The provider is designed to work with both traditional Cassandra clusters and newer versions of Scylla/Cassandra by supporting a configurable system keyspace setting.

## Provider Installation

Download or compile the provider from the source code. To install it manually:
1. Build the provider binary.
2. Place the binary in the appropriate Terraform plugin directory.
3. In your Terraform configuration, reference the provider (see below).

## Provider Configuration

Add a provider block to your Terraform configuration. At minimum, you must configure the connection settings as well as the system keyspace name.

```hcl
provider "cassandra" {
  username              = "admin"
  password              = "admin_password"
  host                  = "127.0.0.1"
  port                  = 9042
  system_keyspace_name  = "system_auth" # or "system" for new Scylla/Cassandra versions

  # Optional settings:
  # hosts               = ["127.0.0.1", "192.168.1.10"]
  # host_filter         = false
  # connection_timeout  = 1000
  # use_ssl             = false
  # root_ca             = "<pem_string>"
  # min_tls_version     = "TLS1.2"
  # protocol_version    = 4
  # consistency         = "QUORUM"
  # cql_version         = "3.0.0"
  # keyspace            = "initial_keyspace"
  # disable_initial_host_lookup = false
  # enable_host_verification    = true
}
