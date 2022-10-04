package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	ddbclient "ddb-export/pkg/client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func createLocalClient() *dynamodb.Client {
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

func createStagingClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("eu-west-2"),
		config.WithSharedConfigProfile("convrs"),
	)
	if err != nil {
		panic(err)
	}
	return dynamodb.NewFromConfig(cfg)
}

func convertAttrValue(value ddbtypes.AttributeValue) (rec map[string]any) {
	rec = map[string]any{}
	// TODO: support all remaining types
	switch t := value.(type) {
	case *ddbtypes.AttributeValueMemberBOOL:
		rec["BOOL"] = t.Value
	case *ddbtypes.AttributeValueMemberL:
		var values = []map[string]any{}

		for _, v := range t.Value {
			values = append(values, convertAttrValue(v))
		}
		rec["L"] = values
	case *ddbtypes.AttributeValueMemberM:
		var r = map[string]any{}
		for k, v := range t.Value {
			r[k] = convertAttrValue(v)
		}
		rec["M"] = r
	case *ddbtypes.AttributeValueMemberN:
		rec["N"] = t.Value
	case *ddbtypes.AttributeValueMemberS:
		rec["S"] = t.Value
	}

	return rec
}

func main() {
	bStaging := flag.Bool("staging", false, "use staging remote db")
	flag.Parse()
	var client *dynamodb.Client
	var table string

	log.Println(*bStaging)

	if *bStaging {
		client = ddbclient.CreateStagingClient()
		table = "convrs-backend-staging-db-users"
	} else {
		table = "convrsUsers"
		client = ddbclient.CreateLocalClient()
	}

	var startKey map[string]ddbtypes.AttributeValue
	var result = []map[string]any{}

	for {
		opt := dynamodb.ScanInput{
			TableName:         &table,
			ExclusiveStartKey: startKey,
		}

		output, err := client.Scan(context.Background(), &opt)

		if err != nil {
			panic(err)
		}

		for _, item := range output.Items {
			var v = map[string]any{}

			for attr, value := range item {
				v[attr] = convertAttrValue(value)
			}

			result = append(result, v)
		}

		startKey = output.LastEvaluatedKey
		if startKey == nil {
			break
		}
	}

	js, err := json.Marshal(result)

	if err != nil {
		panic(err)
	}
	fmt.Println(string(js))
}
