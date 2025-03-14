---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "cassandra_grant Resource - terraform-provider-cassandra"
subcategory: ""
description: |-
  Manage Grants within your cassandra cluster
---

# cassandra_grant (Resource)

Manage Grants within your cassandra cluster

## Example Usage

```terraform
resource "cassandra_grant" "all_access_to_keyspace" {
  privilege     = "all"
  resource_type = "keyspace"
  keyspace_name = "test"
  grantee       = "migration"
}
```

<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `grantee` (String) role name who we are granting privilege(s) to
- `privilege` (String) One of select, create, alter, drop, modify, authorize, describe, execute
- `resource_type` (String) Resource type we are granting privilege to. Must be one of all functions, all functions in keyspace, function, all keyspaces, keyspace, table, all roles, role, roles, mbean, mbeans, all mbeans

### Optional

- `function_name` (String) keyspace qualifier to the resource, only applicable for resource all functions in keyspace, function, keyspace, table
- `keyspace_name` (String) keyspace qualifier to the resource, only applicable for resource all functions in keyspace, function, keyspace, table
- `mbean_name` (String) name of mbean, only applicable for resource mbean
- `mbean_pattern` (String) pattern for selecting mbeans, only valid for resource mbeans
- `role_name` (String) name of the role, applicable only for resource role
- `table_name` (String) name of the table, applicable only for resource table

### Read-Only

- `id` (String) The ID of this resource.
