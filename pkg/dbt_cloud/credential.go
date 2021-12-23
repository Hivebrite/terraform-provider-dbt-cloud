package dbt_cloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const (
	TypeBigQueryCredential = "bigquery"
)

type CredentialListResponse struct {
	Data   []Credential   `json:"data"`
	Status ResponseStatus `json:"status"`
}

type CredentialResponse struct {
	Data   Credential     `json:"data"`
	Status ResponseStatus `json:"status"`
}

type Credential struct {
	ID         *int   `json:"id"`
	Account_Id int    `json:"account_id"`
	Project_Id int    `json:"project_id"`
	Type       string `json:"type"`
	State      int    `json:"state"`
	Threads    int    `json:"threads"`
	User       string `json:"user"`
	Password   string `json:"password"`
	Auth_Type  string `json:"auth_type"`
	Schema     string `json:"schema"`
}

func (c *Client) GetCredential(projectId int, credentialId int) (*Credential, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/credentials/", HostURL, c.AccountID, projectId), nil)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	credentialListResponse := CredentialListResponse{}
	err = json.Unmarshal(body, &credentialListResponse)
	if err != nil {
		return nil, err
	}

	for i, credential := range credentialListResponse.Data {
		if *credential.ID == credentialId {
			return &credentialListResponse.Data[i], nil
		}
	}

	return nil, fmt.Errorf("did not find credential ID %d in project ID %d", credentialId, projectId)
}

func (c *Client) CreateCredential(credential *Credential, projectId int) (*Credential, error) {
	credential.Account_Id = c.AccountID
	credential.Project_Id = projectId
	credential.State = 1 // TODO: make variable

	newCredentialData, err := json.Marshal(credential)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/credentials/", HostURL, c.AccountID, projectId), strings.NewReader(string(newCredentialData)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	credentialResponse := CredentialResponse{}
	err = json.Unmarshal(body, &credentialResponse)
	if err != nil {
		return nil, err
	}

	return &credentialResponse.Data, nil
}

func (c *Client) UpdateCredential(projectId int, credentialId int, credential Credential) (*Credential, error) {
	credentialData, err := json.Marshal(credential)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/accounts/%d/projects/%d/credentials/%d", HostURL, c.AccountID, projectId, credentialId), strings.NewReader(string(credentialData)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	credentialResponse := CredentialResponse{}
	err = json.Unmarshal(body, &credentialResponse)
	if err != nil {
		return nil, err
	}

	return &credentialResponse.Data, nil
}
