package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gthesheep/terraform-provider-dbt-cloud/pkg/dbt_cloud"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourceBigQueryConnection() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceConnectionCreate,
		ReadContext:   resourceConnectionRead,
		UpdateContext: resourceConnectionUpdate,
		DeleteContext: resourceConnectionDelete,

		Schema: map[string]*schema.Schema{

			"name": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the connection",
			},
			"project_id": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				Description: "Project ID to link the connection to",
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
							Default:     1,
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
							Description: "JSON string representing the private key of the Service Account that will connect to the BigQuery",
						},
					},
				},
			},
		},

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func parseBigQueryConnection(d *schema.ResourceData) (*dbt_cloud.BigQueryConnection, error) {
	name := d.Get("name").(string)
	projectId := d.Get("project_id").(int)

	details := d.Get("details").(*schema.Set).List()[0].(map[string]interface{})
	retries := details["retries"].(int)
	location := details["location"].(string)
	timeoutSeconds := details["timeout_seconds"].(int)
	serviceAccountPrivateKey := details["service_account_private_key"].(string)

	detailObject := dbt_cloud.BigQueryConnectionDetails{}
	err := json.Unmarshal([]byte(serviceAccountPrivateKey), &detailObject)
	if err != nil {
		return nil, err
	}

	detailObject.Retries = retries
	detailObject.Location = location
	detailObject.TimeoutSeconds = timeoutSeconds

	return &dbt_cloud.BigQueryConnection{
		Name:      name,
		ProjectID: projectId,
		Type:      dbt_cloud.TypeBigQuery,
		Details:   detailObject,
		State:     1, // TODO: make variable
	}, nil

}

func resourceConnectionCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	var diags diag.Diagnostics

	parsedObject, err := parseBigQueryConnection(d)
	if err != nil {
		return diag.FromErr(err)
	}

	connectionCreated, err := c.CreateBigQueryConnection(parsedObject)

	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(fmt.Sprintf("%d%s%d", connectionCreated.ProjectID, dbt_cloud.ID_DELIMITER, *connectionCreated.ID))

	resourceConnectionRead(ctx, d, m)

	return diags
}

func resourceConnectionRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	var diags diag.Diagnostics

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	connectionId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	connection, err := c.GetBigQueryConnection(projectId, connectionId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("project_id", connection.ProjectID); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("name", connection.Name); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("details", connection.Details); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceConnectionUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	connectionId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	parsedObject, err := parseBigQueryConnection(d)
	if err != nil {
		return diag.FromErr(err)
	}

	_, err = c.UpdateBigQueryConnection(projectId, connectionId, *parsedObject)
	if err != nil {
		return diag.FromErr(err)
	}

	return resourceConnectionRead(ctx, d, m)
}

func resourceConnectionDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	var diags diag.Diagnostics

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	connectionId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	err = c.DeleteConnection(projectId, connectionId)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
