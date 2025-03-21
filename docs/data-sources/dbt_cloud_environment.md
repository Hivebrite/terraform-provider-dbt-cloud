---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "dbt_cloud_environment Data Source - terraform-provider-dbt-cloud"
subcategory: ""
description: |-
  
---

# dbt_cloud_environment (Data Source)





<!-- schema generated by tfplugindocs -->
## Schema

### Required

- **environment_id** (Number) Project ID to create the environment in
- **project_id** (Number) Project ID to create the environment in

### Optional

- **id** (String) The ID of this resource.

### Read-Only

- **credential_id** (Number) Credential ID to create the environment with
- **custom_branch** (String) Which custom branch to use in this environment
- **dbt_version** (String) Version number of dbt to use in this environment
- **is_active** (Boolean) Whether the environment is active
- **name** (String) Environment name
- **type** (String) The type of environment (must be either development or deployment)
- **use_custom_branch** (Boolean) Whether to use a custom git branch in this environment


