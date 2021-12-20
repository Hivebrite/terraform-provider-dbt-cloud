package resources

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"github.com/gthesheep/terraform-provider-dbt-cloud/pkg/dbt_cloud"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var projectSchema = map[string]*schema.Schema{
	"name": &schema.Schema{
		Type:        schema.TypeString,
		Required:    true,
		Description: "Project name",
	},
	"dbt_project_subdirectory": &schema.Schema{
		Type:        schema.TypeString,
		Optional:    true,
		Description: "DBT project subdirectory path",
	},
	"connection_id": &schema.Schema{
		Type:        schema.TypeInt,
		Optional:    true,
		Computed:    true,
		Description: "Connection ID",
		ConflictsWith: []string{
			dbt_cloud.TypeBigQueryProject,
		},
	},
	"repository_id": &schema.Schema{
		Type:        schema.TypeInt,
		Optional:    true,
		Description: "Repository ID",
	},
	dbt_cloud.TypeBigQueryProject: {
		Type:        schema.TypeList,
		Optional:    true,
		Description: "Project using a BigQUery connection",
		ConflictsWith: []string{
			"connection_id",
		},
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"name": {
					Type:        schema.TypeString,
					Required:    true,
					Description: "Connection name to be used",
				},
				"details": &schema.Schema{
					Required:    true,
					Type:        schema.TypeSet,
					Description: "Details of the connection to be made",
					MinItems:    1,
					MaxItems:    1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"retries": {
								Type:        schema.TypeInt,
								Optional:    true,
								Default:     1,
								Description: "The number of times to retry queries that fail with BigQuery internal errors.",
							},
							"timeout_seconds": {
								Type:        schema.TypeInt,
								Optional:    true,
								Default:     300,
								Description: "Support for the timeout_seconds configuration will be removed in a future release of dbt.",
							},
							"location": {
								Type:        schema.TypeString,
								Optional:    true,
								Default:     nil,
								Description: "Location to create new Datasets in",
							},
							"service_account_private_key": {
								Type:        schema.TypeString,
								Required:    true,
								Sensitive:   true,
								Description: "JSON string representing the private key of the Service Account that will connect to the BigQuery",
							},
						},
					},
				},
			},
		},
	},
}

func ResourceProject() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceProjectCreate,
		ReadContext:   resourceProjectRead,
		UpdateContext: resourceProjectUpdate,
		DeleteContext: resourceProjectDelete,

		Schema: projectSchema,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceProjectRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	var diags diag.Diagnostics

	projectID := d.Id()

	project, err := c.GetProject(projectID)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("name", project.Name); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("dbt_project_subdirectory", project.DbtProjectSubdirectory); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("connection_id", project.ConnectionID); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("repository_id", project.RepositoryID); err != nil {
		return diag.FromErr(err)
	}

	connection := project.Connection
	if connection != nil {
		log.Printf("CONNECTION IS NOT EMPTY, %s", connection.Type)
		switch connection.Type {
		case dbt_cloud.TypeBigQueryProject:
			val := map[string]interface{}{
				"name": connection.Name,
				"details": []map[string]interface{}{{
					"retries":                     connection.Details.Retries,
					"location":                    connection.Details.Location,
					"timeout_seconds":             connection.Details.TimeoutSeconds,
					"service_account_private_key": connection.Details.PrivateKeyID,
				}},
			}

			if err := d.Set(dbt_cloud.TypeBigQueryProject, []interface{}{val}); err != nil {
				return diag.FromErr(err)
			}

		}
	}

	return diags
}

func resourceProjectCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	var diags diag.Diagnostics
	var connection *dbt_cloud.Connection

	name := d.Get("name").(string)
	dbtProjectSubdirectory := d.Get("dbt_project_subdirectory").(string)
	connectionID := d.Get("connection_id").(int)
	repositoryID := d.Get("repository_id").(int)

	if x := ResourceDataInterfaceMap(d, dbt_cloud.TypeBigQuery); len(x) != 0 {
		details := x["details"].(*schema.Set).List()[0].(map[string]interface{})
		serviceAccountPrivateKey := details["service_account_private_key"].(string)

		detailObject := dbt_cloud.ConnectionDetails{}
		err := json.Unmarshal([]byte(serviceAccountPrivateKey), &detailObject)
		if err != nil {
			return diag.FromErr(err)
		}

		detailObject.Retries = details["retries"].(int)
		detailObject.Location = details["location"].(string)
		detailObject.TimeoutSeconds = details["timeout_seconds"].(int)

		connection = &dbt_cloud.Connection{
			Name:    x["name"].(string),
			Type:    dbt_cloud.TypeBigQuery,
			Details: detailObject,
		}

	}

	p, err := c.CreateProject(name, dbtProjectSubdirectory, connectionID, repositoryID, connection)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(strconv.Itoa(*p.ID))

	resourceProjectRead(ctx, d, m)

	return diags
}

func resourceProjectUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)
	projectID := d.Id()

	if d.HasChange("name") || d.HasChange("dbt_project_subdirectory") || d.HasChange("connection_id") || d.HasChange("repository_id") {
		project, err := c.GetProject(projectID)
		if err != nil {
			return diag.FromErr(err)
		}

		if d.HasChange("name") {
			name := d.Get("name").(string)
			project.Name = name
		}
		if d.HasChange("dbt_project_subdirectory") {
			dbtProjectSubdirectory := d.Get("dbt_project_subdirectory").(string)
			project.DbtProjectSubdirectory = &dbtProjectSubdirectory
		}
		if d.HasChange("connection_id") {
			connectionID := d.Get("connection_id").(int)
			project.ConnectionID = &connectionID
		}
		if d.HasChange("repository_id") {
			repositoryID := d.Get("repository_id").(int)
			project.RepositoryID = &repositoryID
		}

		_, err = c.UpdateProject(projectID, *project)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	return resourceProjectRead(ctx, d, m)
}

func resourceProjectDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)
	projectID := d.Id()

	var diags diag.Diagnostics

	project, err := c.GetProject(projectID)
	if err != nil {
		return diag.FromErr(err)
	}

	project.State = dbt_cloud.STATE_DELETED
	_, err = c.UpdateProject(projectID, *project)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
