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
	TypeGithubRepository = "github"
)

type RepositoryResponse struct {
	Status ResponseStatus `json:"status"`
	Data   Repository     `json:"data"`
}

type Repository struct {
	ID        *int   `json:"id,omitempty"`
	AccountID int    `json:"account_id"`
	ProjectID int    `json:"project_id"`
	RemoteURL string `json:"remote_url"`
	State     int    `json:"state"`
}

func (c *Client) GetRepository(repositoryID string) (*Repository, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/v3/accounts/%s/repositories/%s/", c.HostURL, strconv.Itoa(c.AccountID), repositoryID), nil)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	repositoryResponse := RepositoryResponse{}
	err = json.Unmarshal(body, &repositoryResponse)
	if err != nil {
		return nil, err
	}

	return &repositoryResponse.Data, nil
}

func (c *Client) CreateRepository(repository *Repository, projectID int) (*Repository, error) {
	url := fmt.Sprintf("%s/v3/accounts/%s/projects/%d/repositories/", c.HostURL, strconv.Itoa(c.AccountID), projectID)
	repository.AccountID = c.AccountID
	repository.ProjectID = projectID
	repository.State = STATE_ACTIVE

	newRepository, err := c.createUpdateRepository(repository, url)
	if err != nil {
		return nil, err
	}

	return newRepository, nil
}

func (c *Client) UpdateRepository(repository *Repository, projectID int) (*Repository, error) {
	url := fmt.Sprintf("%s/v3/accounts/%s/projects/%d/repositories/%d/", c.HostURL, strconv.Itoa(c.AccountID), projectID, *repository.ID)
	repository.AccountID = c.AccountID
	repository.ProjectID = projectID

	updatedRepository, err := c.createUpdateRepository(repository, url)
	if err != nil {
		return nil, err
	}

	return updatedRepository, nil
}

func (c *Client) createUpdateRepository(repository *Repository, url string) (*Repository, error) {
	repositoryData, err := json.Marshal(repository)
	if err != nil {
		return nil, err
	}
	log.Println("Repository Payload: %s (url: %s)", string(repositoryData), url)

	req, err := http.NewRequest("POST", url, strings.NewReader(string(repositoryData)))
	if err != nil {
		return nil, err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	repositoryResponse := RepositoryResponse{}
	err = json.Unmarshal(body, &repositoryResponse)
	if err != nil {
		return nil, err
	}
	return &repositoryResponse.Data, nil
}

func (c *Client) DeleteRepository(repositoryID int, projectID int) error {
	log.Println("Repository Destroy (ID: %d)", repositoryID)

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/v3/accounts/%s/projects/%d/repositories/%d/", c.HostURL, strconv.Itoa(c.AccountID), projectID, repositoryID), nil)
	if err != nil {
		return err
	}

	_, err = c.doRequest(req)
	if err != nil {
		return err
	}

	return nil
}
