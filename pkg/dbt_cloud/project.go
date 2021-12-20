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
	TypeBigQueryProject = "bigquery"
)

type Project struct {
	ID                     *int        `json:"id,omitempty"`
	Name                   string      `json:"name"`
	DbtProjectSubdirectory *string     `json:"dbt_project_subdirectory,omitempty"`
	ConnectionID           *int        `json:"connection_id,integer,omitempty"`
	Connection             *Connection `json:"connection,omitempty"`
	RepositoryID           *int        `json:"repository_id,integer,omitempty"`
	State                  int         `json:"state"`
	AccountID              int         `json:"account_id"`
}

type Connection struct {
	ID                      *int              `json:"id"`
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

type ProjectListResponse struct {
	Data   []Project      `json:"data"`
	Status ResponseStatus `json:"status"`
}

type ProjectResponse struct {
	Data   Project        `json:"data"`
	Status ResponseStatus `json:"status"`
}

func (c *Client) GetProject(projectID string) (*Project, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/v3/accounts/%s/projects/%s/", c.HostURL, strconv.Itoa(c.AccountID), projectID), nil)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	projectResponse := ProjectResponse{}
	err = json.Unmarshal(body, &projectResponse)
	if err != nil {
		return nil, err
	}

	log.Printf("GETPROJECT: %s", projectResponse.Data)
	return &projectResponse.Data, nil
}

func (c *Client) CreateProject(name string, dbtProjectSubdirectory string, connectionID int, repositoryID int, connection *Connection) (*Project, error) {
	newProject := Project{
		Name:      name,
		State:     1,
		AccountID: c.AccountID,
	}
	if dbtProjectSubdirectory != "" {
		newProject.DbtProjectSubdirectory = &dbtProjectSubdirectory
	}
	if connectionID != 0 {
		newProject.ConnectionID = &connectionID
	}
	if repositoryID != 0 {
		newProject.RepositoryID = &repositoryID
	}

	newProjectData, err := json.Marshal(newProject)
	if err != nil {
		return nil, err
	}
	log.Println(string(newProjectData))

	// Create the project
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%s/projects/", c.HostURL, strconv.Itoa(c.AccountID)), strings.NewReader(string(newProjectData)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	projectResponse := ProjectResponse{}
	err = json.Unmarshal(body, &projectResponse)
	if err != nil {
		return nil, err
	}
	projectReturned := projectResponse.Data

	var connectionResponse *Connection
	if connection != nil {
		projectCreated := projectResponse.Data

		projectID := projectCreated.ID
		// Create the connection
		connectionResponse, err = c.CreateConnection(connection, *projectID)
		if err != nil {
			// TODO: delete project
			return nil, err
		}

		// Update project with connection ID
		projectCreated.ConnectionID = connectionResponse.ID
		pProjectReturned, err := c.UpdateProject(strconv.Itoa(*projectID), projectCreated)
		projectReturned = *pProjectReturned

		if err != nil {
			// TODO: delete project
			return nil, err
		}

	}

	projectReturned.Connection = connectionResponse
	return &projectReturned, nil
}

func (c *Client) CreateConnection(connection *Connection, projectID int) (*Connection, error) {
	connection.AccountID = c.AccountID
	connection.ProjectID = projectID

	connectionData, err := json.Marshal(connection)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%s/projects/%d/connections/", c.HostURL, strconv.Itoa(c.AccountID), projectID), strings.NewReader(string(connectionData)))

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

func (c *Client) UpdateProject(projectID string, project Project) (*Project, error) {
	projectData, err := json.Marshal(project)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%s/projects/%s/", c.HostURL, strconv.Itoa(c.AccountID), projectID), strings.NewReader(string(projectData)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	projectResponse := ProjectResponse{}
	err = json.Unmarshal(body, &projectResponse)
	if err != nil {
		return nil, err
	}

	return &projectResponse.Data, nil
}
