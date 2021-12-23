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
			"type": &schema.Schema{
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The credential Type",
			},
			"num_threads": &schema.Schema{
				Type:        schema.TypeInt,
				Required:    true,
				Description: "Number of threads to use",
			},
			dbt_cloud.TypeBigQueryCredential: {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Project using BigQuery credentials",
				ConflictsWith: []string{
					dbt_cloud.TypeSnowflakeCredential,
				},
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"schema": &schema.Schema{
							Type:        schema.TypeString,
							Required:    true,
							Description: "Default schema name",
						},
					},
				},
			},
			dbt_cloud.TypeSnowflakeCredential: {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Project using Snowflake credentials",
				ConflictsWith: []string{
					dbt_cloud.TypeBigQueryCredential,
				},
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"schema": &schema.Schema{
							Type:        schema.TypeString,
							Required:    true,
							Description: "Default schema name",
						},
						"auth_type": &schema.Schema{
							Type:        schema.TypeString,
							Required:    true,
							Description: "The type of Snowflake credential ('password' only currently supported in Terraform)",
							ValidateFunc: func(val interface{}, key string) (warns []string, errs []error) {
								type_ := val.(string)
								switch type_ {
								case
									"password":
									return
								}
								errs = append(errs, fmt.Errorf("%q must be password, got: %q", key, type_))
								return
							},
						},
						"user": &schema.Schema{
							Type:        schema.TypeString,
							Required:    true,
							Description: "Username for Snowflake",
						},
						"password": &schema.Schema{
							Type:        schema.TypeString,
							Required:    true,
							Sensitive:   true,
							Description: "Password for Snowflake",
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

func resourceCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	projectId := d.Get("project_id").(int)

	newCredential := dbt_cloud.Credential{
		Threads: d.Get("num_threads").(int),
		State:   1, // TODO: MAKE VARIABLE
	}

	if x := ResourceDataInterfaceMap(d, dbt_cloud.TypeBigQueryCredential); len(x) != 0 {
		newCredential.Type = dbt_cloud.TypeBigQueryCredential
		newCredential.Schema = x["schema"].(string)
	} else if x := ResourceDataInterfaceMap(d, dbt_cloud.TypeSnowflakeCredential); len(x) != 0 {
		newCredential.Type = dbt_cloud.TypeSnowflakeCredential
		newCredential.Schema = x["schema"].(string)
		newCredential.Auth_Type = x["auth_type"].(string)
		newCredential.User = x["user"].(string)
		newCredential.Password = x["password"].(string)
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
	var val map[string]interface{}

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	credentialId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	credential, err := c.GetCredential(projectId, credentialId)
	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("credential_id", credentialId); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("is_active", credential.State == dbt_cloud.STATE_ACTIVE); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("project_id", credential.Project_Id); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("type", credential.Type); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set("num_threads", credential.Threads); err != nil {
		return diag.FromErr(err)
	}

	switch credential.Type {
	case dbt_cloud.TypeBigQueryCredential:
		val = map[string]interface{}{
			"schema": credential.Schema,
		}
	case dbt_cloud.TypeSnowflakeCredential:
		val = map[string]interface{}{
			"schema":    credential.Schema,
			"auth_type": credential.Auth_Type,
			"user":      credential.User,
			"password":  credential.Password,
		}
	}

	for _, key := range []string{dbt_cloud.TypeBigQueryCredential, dbt_cloud.TypeSnowflakeCredential} {
		if key != credential.Type {
			d.Set(key, nil)
			continue
		}

		if err := d.Set(key, []interface{}{val}); err != nil {
			return diag.FromErr(err)
		}
	}
	return diags
}

func resourceCredentialUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*dbt_cloud.Client)

	projectId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[0])
	if err != nil {
		return diag.FromErr(err)
	}

	credentialId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	if d.HasChange(dbt_cloud.TypeBigQueryCredential) || d.HasChange(dbt_cloud.TypeSnowflakeCredential) || d.HasChange("num_threads") {
		credential, err := c.GetCredential(projectId, credentialId)
		if err != nil {
			return diag.FromErr(err)
		}

		if d.HasChange(dbt_cloud.TypeBigQueryCredential) {
			if x := ResourceDataInterfaceMap(d, dbt_cloud.TypeBigQueryCredential); len(x) != 0 {
				schema := x["schema"].(string)
				credential.Schema = schema
			}
		} else if d.HasChange(dbt_cloud.TypeSnowflakeCredential) {
			if x := ResourceDataInterfaceMap(d, dbt_cloud.TypeSnowflakeCredential); len(x) != 0 {

				authType := x["auth_type"].(string)
				schema := x["schema"].(string)
				user := x["user"].(string)
				password := x["password"].(string)

				credential.User = user
				credential.Password = password
				credential.Schema = schema
				credential.Auth_Type = authType
			}
		}

		if d.HasChange("num_threads") {
			numThreads := d.Get("num_threads").(int)
			credential.Threads = numThreads
		}

		_, err = c.UpdateCredential(projectId, credentialId, *credential)
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

	credentialId, err := strconv.Atoi(strings.Split(d.Id(), dbt_cloud.ID_DELIMITER)[1])
	if err != nil {
		return diag.FromErr(err)
	}

	credential, err := c.GetCredential(projectId, credentialId)
	if err != nil {
		return diag.FromErr(err)
	}

	credential.State = dbt_cloud.STATE_DELETED
	_, err = c.UpdateCredential(projectId, credentialId, *credential)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
