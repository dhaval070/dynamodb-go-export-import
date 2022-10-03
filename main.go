package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

func main() {
	fmt.Println("vim-go")
	client := CreateLocalClient()

	table := "convrsUsers"

	opt := dynamodb.ScanInput{
		TableName: &table,
	}
	output, err := client.Scan(context.Background(), &opt)

	if err != nil {
		panic(err)
	}

	var items = make([]map[string]any, 0, 1)

	for _, v := range output.Items {
		js, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		var obj map[string]any
		err = json.Unmarshal(js, &obj)

		if err != nil {
			panic(err)
		}

		items = append(items, obj)
		fmt.Println(string(js))
		fmt.Println("---------------------")
	}
	str, err := json.Marshal(items)
	fmt.Println(string(str))
}
