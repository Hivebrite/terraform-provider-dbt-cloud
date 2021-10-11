resource "dbt_cloud_job" "test" {
  environment_id = <environment_id>
  execute_steps = [
    "dbt test"
  ]
  generate_docs        = false
  is_active            = true
  name                 = "Test"
  num_threads          = 64
  project_id           = <project_id>
  run_generate_sources = false
  target_name          = "default"
  triggers = {
    "custom_branch_only" : true,
    "github_webhook" : false,
    "schedule" : false
  }
}
