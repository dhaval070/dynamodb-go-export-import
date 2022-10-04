package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var tableName = "convrsUsersNew"

func CreateLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func createTable(client *dynamodb.Client) {
	var id = "id"
	var readUnits int64 = 1000
	var writeUnits int64 = 1000

	opt := dynamodb.CreateTableInput{
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{
				AttributeName: &id,
				AttributeType: ddbtypes.ScalarAttributeTypeS,
			},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{
				AttributeName: &id,
				KeyType:       ddbtypes.KeyTypeHash,
			},
		},
		TableName: &tableName,
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  &readUnits,
			WriteCapacityUnits: &writeUnits,
		},
	}

	output, err := client.CreateTable(context.Background(), &opt)
	if err != nil {
		panic(err)
	}
	fmt.Println(output)
}

func importCsv(client *dynamodb.Client, fd io.Reader) {
	reader := csv.NewReader(fd)

	items, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	head := items[0]
	//fmt.Println(head)

	var records = []map[string]ddbtypes.AttributeValue{}
	var opt = dynamodb.PutItemInput{
		TableName: &tableName,
	}
	for _, item := range items[1:] {
		var obj = map[string]ddbtypes.AttributeValue{}
		for i, h := range head {
			if i >= len(item) {
				continue
			}
			var js map[string]ddbtypes.AttributeValue
			err := json.Unmarshal([]byte(item[i]), &js)
			if err != nil {
				obj[h] = &ddbtypes.AttributeValueMemberS{Value: item[i]}
			} else {
				obj[h] = &ddbtypes.AttributeValueMemberM{Value: js}
			}
		}
		records = append(records, obj)
		opt.Item = obj
		output, err := client.PutItem(context.Background(), &opt)
		if err != nil {
			panic(err)
		}
		fmt.Println(output)
	}

}

func main() {
	dir, _ := os.Getwd()
	fmt.Println(dir)
	client := CreateLocalClient()

	createTable(client)

	fd, err := os.Open("results.csv")
	if err != nil {
		panic(err)
	}

	importCsv(client, fd)
}
