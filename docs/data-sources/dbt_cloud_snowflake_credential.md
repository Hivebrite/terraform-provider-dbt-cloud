---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "dbt_cloud_snowflake_credential Data Source - terraform-provider-dbt-cloud"
subcategory: ""
description: |-
  
---

# dbt_cloud_snowflake_credential (Data Source)





<!-- schema generated by tfplugindocs -->
## Schema

### Required

- **credential_id** (Number) Credential ID
- **project_id** (Number) Project ID

### Optional

- **id** (String) The ID of this resource.

### Read-Only

- **auth_type** (String) The type of Snowflake credential ('password' only currently supported in Terraform)
- **is_active** (Boolean) Whether the Snowflake credential is active
- **num_threads** (Number) Number of threads to use
- **password** (String, Sensitive) Password for Snowflake
- **schema** (String) Default schema name
- **user** (String) Username for Snowflake


