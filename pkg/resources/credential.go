package resources

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gthesheep/terraform-provider-dbt-cloud/pkg/dbt_cloud"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourceCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceCredentialCreate,
		ReadContext:   resourceCredentialRead,
		UpdateContext: resourceCredentialUpdate,
		DeleteContext: resourceCredentialDelete,

		Schema: map[string]*schema.Schema{
			"is_active": &schema.Schema{
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "Whether the credential is active",
			},
			"project_id": &schema.Schema{
				Type:        schema.TypeInt,
				Required:    true,
				Description: "Project ID to create the  credential in",
			},
			"credential_id": &schema.Schema{
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The system credential ID",
			},
			"schema": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				Description: "Default schema name",
			},
			"num_threads": &schema.Schema{
				Type:        schema.TypeInt,
				Required:    true,
				Description: "Number of threads to use",
			},
			dbt_cloud.TypeBigQueryCredential: {
				Type:          schema.TypeList,
				Optional:      true,
				Description:   "Project using a BigQuery credentials",
				ConflictsWith: []string{},
				MaxItems:      1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{},
				},
			},
		},

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	projectId := d.Get("project_id").(int)

	newCredential := dbt_cloud.Credential{
		Schema:  d.Get("schema").(string),
		Threads: d.Get("num_threads").(int),
		State:   1, // TODO: MAKE VARIABLE
	}

	if x := ResourceDataInterfaceMap(d, dbt_cloud.TypeBigQueryCredential); len(x) != 0 {
		newCredential.Type = dbt_cloud.TypeBigQueryCredential
	}

	credential, err := c.CreateCredential(&newCredential, projectId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(fmt.Sprintf("%d%s%d", credential.Project_Id, dbt_cloud.ID_DELIMITER, *credential.ID))

	resourceCredentialRead(ctx, d, m)

	return diags
}

func resourceCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	CredentialId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	Credential, err := c.GetCredential(projectId, CredentialId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("credential_id", CredentialId); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("is_active", Credential.State == dbt_cloud.STATE_ACTIVE); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("project_id", Credential.Project_Id); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("schema", Credential.Schema); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("num_threads", Credential.Threads); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceCredentialUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	CredentialId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	if d.HasChange("schema") || d.HasChange("num_threads") {
		Credential, err := c.GetCredential(projectId, CredentialId)
		if err != nil {
			return diag.FromErr(err)
		}

		if d.HasChange("schema") {
			schema := d.Get("schema").(string)
			Credential.Schema = schema
		}
		if d.HasChange("num_threads") {
			numThreads := d.Get("num_threads").(int)
			Credential.Threads = numThreads
		}

		_, err = c.UpdateCredential(projectId, CredentialId, *Credential)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	return resourceCredentialRead(ctx, d, m)
}

func resourceCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	var diags diag.Diagnostics

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	CredentialId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	Credential, err := c.GetCredential(projectId, CredentialId)
	if err != nil {
		return diag.FromErr(err)
	}

	Credential.State = dbt_cloud.STATE_DELETED
	_, err = c.UpdateCredential(projectId, CredentialId, *Credential)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
