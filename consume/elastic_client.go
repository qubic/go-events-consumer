package consume

import (
	"bytes"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"log"
)

type ElasticClient struct {
	esClient  *elasticsearch.Client
	indexName string
}

func NewElasticClient(esClient *elasticsearch.Client, indexName string) *ElasticClient {
	return &ElasticClient{
		esClient:  esClient,
		indexName: indexName,
	}
}

func (c *ElasticClient) IndexEvent(data []byte) error {
	// TODO replace with bulk insert
	response, err := c.esClient.Index(c.indexName, bytes.NewReader(data))
	if err != nil || response == nil {
		return errors.Wrap(err, "Error indexing event")
	} else if response.IsError() {
		return errors.Errorf("Error indexing event. Unexpected status code [%s]", response.String())
	} else if response.HasWarnings() {
		log.Printf("Warning indexing event: %v", response.Warnings())
		log.Printf("Response: %s", response.String())
	}
	return nil
}

type FakeElasticClient struct {
}

func (c *FakeElasticClient) IndexEvent(_ []byte) error {
	return nil
}
