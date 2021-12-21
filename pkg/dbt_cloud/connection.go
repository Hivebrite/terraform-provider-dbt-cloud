package dbt_cloud

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const (
	TypeBigQueryConnection = "bigquery"
)

type Connection struct {
	ID                      *int              `json:"id,omitempty"`
	AccountID               int               `json:"account_id"`
	ProjectID               int               `json:"project_id"`
	Name                    string            `json:"name"`
	Type                    string            `json:"type"`
	CreatedByID             int               `json:"created_by_id"`
	CreatedByServiceTokenID int               `json:"created_by_service_token_id"`
	Details                 ConnectionDetails `json:"details"`
	State                   int               `json:"state"`
	CreatedAt               string            `json:"created_at"`
	UpdatedAt               string            `json:"updated_at"`
}

type ConnectionDetails struct {
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

type ConnectionResponse struct {
	Status ResponseStatus `json:"status"`
	Data   Connection     `json:"data"`
}

func (c *Client) GetConnection(connectionID int, projectID int) (*Connection, error) {
	url := fmt.Sprintf("%s/v3/accounts/%s/projects/%d/connections/%d/", c.HostURL, strconv.Itoa(c.AccountID), projectID, connectionID)

	log.Println("Connection GET (url: %s)", url)

	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	connectionResponse := ConnectionResponse{}
	err = json.Unmarshal(body, &connectionResponse)
	if err != nil {
		return nil, err
	}
	return &connectionResponse.Data, nil
}

func (c *Client) CreateConnection(connection *Connection, projectID int) (*Connection, error) {
	url := fmt.Sprintf("%s/v3/accounts/%s/projects/%d/connections/", c.HostURL, strconv.Itoa(c.AccountID), projectID)

	connection.AccountID = c.AccountID
	connection.ProjectID = projectID
	connection.State = STATE_ACTIVE

	newConnection, err := c.updateCreateConnection(connection, url)

	if err != nil {
		return nil, err
	}

	return newConnection, nil
}

func (c *Client) UpdateConnection(connection *Connection, projectID int) (*Connection, error) {
	url := fmt.Sprintf("%s/v3/accounts/%s/projects/%d/connections/%d/", c.HostURL, strconv.Itoa(c.AccountID), projectID, *connection.ID)

	connection.AccountID = c.AccountID
	connection.ProjectID = projectID
	updatedConnection, err := c.updateCreateConnection(connection, url)

	if err != nil {
		return nil, err
	}
	return updatedConnection, nil
}

func (c *Client) updateCreateConnection(connection *Connection, url string) (*Connection, error) {
	connectionData, err := json.Marshal(connection)
	if err != nil {
		return nil, err
	}
	log.Println("Connection payload: %s (url: %s)", string(connectionData))

	req, err := http.NewRequest("POST", url, strings.NewReader(string(connectionData)))

	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	connectionResponse := ConnectionResponse{}
	err = json.Unmarshal(body, &connectionResponse)
	if err != nil {
		return nil, err
	}
	return &connectionResponse.Data, nil
}

func (c *Client) DeleteConnection(connectionID int, projectID int) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/v3/accounts/%s/projects/%d/connections/%d/", c.HostURL, strconv.Itoa(c.AccountID), projectID, connectionID), nil)

	if err != nil {
		return err
	}

	_, err = c.doRequest(req)
	if err != nil {
		return err
	}

	return nil
}
