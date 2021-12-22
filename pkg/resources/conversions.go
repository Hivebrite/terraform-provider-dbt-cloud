package resources

import "github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

func ResourceDataInterfaceMap(d *schema.ResourceData, key string) map[string]interface{} {
	if _, ok := d.GetOk(key); ok {
		if v1, ok := d.Get(key).([]interface{}); ok && len(v1) != 0 {
			if v2, ok := v1[0].(map[string]interface{}); ok && v2 != nil {
				return v2
			}
		}
	}

	return map[string]interface{}{}
}
