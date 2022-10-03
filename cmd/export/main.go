package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

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
	client := CreateLocalClient()

	table := "convrsUsers"

	opt := dynamodb.ScanInput{
		TableName: &table,
	}
	output, err := client.Scan(context.Background(), &opt)

	if err != nil {
		panic(err)
	}

	result := []map[string]any{}

	for _, item := range output.Items {
		fmt.Println("---------------------")
		var v = map[string]any{}

		for attr, value := range item {
			v[attr] = convertAttrValue(value)
		}

		result = append(result, v)
	}

	js, err := json.Marshal(result)

	if err != nil {
		panic(err)
	}
	fmt.Println(string(js))
}
