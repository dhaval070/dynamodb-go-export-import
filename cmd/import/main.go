package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	ddbclient "ddb-export/pkg/client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var tableName = "convrsUsersNew"

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

	_, err := client.CreateTable(context.Background(), &opt)
	if err != nil {
		panic(err)
	}
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

func convertAttrValue(item map[string]any) (attr ddbtypes.AttributeValue) {
	for k, v := range item {
		switch k {
		case "BOOL":
			return &ddbtypes.AttributeValueMemberBOOL{Value: v.(bool)}
		case "S":
			return &ddbtypes.AttributeValueMemberS{Value: v.(string)}
		case "M":
			var res = map[string]ddbtypes.AttributeValue{}

			for h, v := range v.(map[string]any) {
				res[h] = convertAttrValue(v.(map[string]any))
			}
			return &ddbtypes.AttributeValueMemberM{Value: res}
		case "N":
			return &ddbtypes.AttributeValueMemberN{Value: v.(string)}
		case "L":
			var vars = []ddbtypes.AttributeValue{}
			for _, vv := range v.([]map[string]any) {
				vars = append(vars, convertAttrValue(vv))
			}
			return &ddbtypes.AttributeValueMemberL{Value: vars}
		default:
			panic("unsupported type")
		}
	}
	return attr
}

func convertItem(item map[string]any) map[string]ddbtypes.AttributeValue {
	var ddbitem = map[string]ddbtypes.AttributeValue{}

	for k, v := range item {
		ddbitem[k] = convertAttrValue(v.(map[string]any))
	}
	return ddbitem
}

func importJson(client *dynamodb.Client, fd io.Reader) {
	data, err := io.ReadAll(fd)
	if err != nil {
		panic(err)
	}
	var items = []map[string]any{}

	err = json.Unmarshal(data, &items)
	if err != nil {
		panic(err)
	}

	for _, item := range items {
		var ddbitem = convertItem(item)
		fmt.Println(ddbitem)

		var opt = &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      ddbitem,
		}

		client.PutItem(context.Background(), opt)
		break
	}
}

func main() {
	bStaging := flag.Bool("staging", false, "use staging remote db")
	sFilename := flag.String("file", "staging.json", "data file to import")

	flag.Parse()
	if *sFilename == "" {
		panic("file is required")
	}

	var client *dynamodb.Client

	log.Println(*bStaging)

	if *bStaging {
		client = ddbclient.CreateStagingClient()
	} else {
		client = ddbclient.CreateLocalClient()
	}

	createTable(client)

	var filename = *sFilename

	fd, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	switch filename[len(filename)-4:] {
	case "json":
		importJson(client, fd)
	case ".csv":
		importCsv(client, fd)
	default:
		panic("unsupported input file format")
	}
}
