package dbt_cloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const (
	TypeBigQuery = "bigquery"
)

type BigQueryConnectionResponse struct {
	Status ResponseStatus     `json:"status"`
	Data   BigQueryConnection `json:"data"`
}

type BigQueryConnectionDetails struct {
	ProjectID               string `json:"project_id"`
	PrivateKey              string `json:"private_key"`
	PrivateKeyID            string `json:"private_key_id"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
	TimeoutSeconds          int    `json:"timeout_seconds"`
	Retries                 int    `json:"retries"`
	Location                string `json:"location"`
	IsConfiguredForOauth    bool   `json:"is_configured_for_oauth"`
}

type BigQueryConnection struct {
	ID                      *int                      `json:"id"`
	AccountID               int                       `json:"account_id"`
	ProjectID               int                       `json:"project_id"`
	Name                    string                    `json:"name"`
	Type                    string                    `json:"type"`
	CreatedByID             int                       `json:"created_by_id"`
	CreatedByServiceTokenID int                       `json:"created_by_service_token_id"`
	Details                 BigQueryConnectionDetails `json:"details"`
	State                   int                       `json:"state"`
	CreatedAt               string                    `json:"created_at"`
	UpdatedAt               string                    `json:"updated_at"`
}

func (c *Client) GetBigQueryConnection(projectId int, connectionId int) (*BigQueryConnection, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/connections/%d", HostURL, c.AccountID, projectId, connectionId), nil)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	connectionResponse := BigQueryConnectionResponse{}
	err = json.Unmarshal(body, &connectionResponse)
	if err != nil {
		return nil, err
	}

	return &connectionResponse.Data, nil
}

func (c *Client) CreateBigQueryConnection(bigQueryConnection *BigQueryConnection) (*BigQueryConnection, error) {
	bigQueryConnection.AccountID = c.AccountID
	newBigQueryConnection, err := json.Marshal(bigQueryConnection)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/connections/", HostURL, c.AccountID), strings.NewReader(string(newBigQueryConnection)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	bigQueryConnectionResponse := BigQueryConnectionResponse{}
	err = json.Unmarshal(body, &bigQueryConnectionResponse)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnectionResponse.Data, nil
}

func (c *Client) UpdateBigQueryConnection(projectId int, connectionId int, bigQueryConnection BigQueryConnection) (*BigQueryConnection, error) {
	bigQueryConnectionData, err := json.Marshal(bigQueryConnection)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/connections/%d", HostURL, c.AccountID, projectId, connectionId), strings.NewReader(string(bigQueryConnectionData)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	bigQueryConnectionResponse := BigQueryConnectionResponse{}
	err = json.Unmarshal(body, &bigQueryConnectionResponse)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnectionResponse.Data, nil
}

func (c *Client) DeleteConnection(projectId int, connectionId int) error {

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/connections/%d/", HostURL, c.AccountID, projectId, connectionId), nil)
	if err != nil {
		return err
	}

	_, err = c.doRequest(req)
	if err != nil {
		return err
	}

	return nil
}
