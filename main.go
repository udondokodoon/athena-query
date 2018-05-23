package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/olekukonko/tablewriter"
)

var (
	readableOpt = flag.Bool("h", false, "show results with human readable table")
	queryOpt    = flag.String("q", "", "specify SQL query string (required)")
)

func main() {
	flag.Parse()

	conf := aws.Config{
		Region: aws.String("ap-northeast-1"),
	}
	sess := session.New(&conf)
	client := athena.New(sess)

	query := *queryOpt

	id, err := execute(client, query)
	if err != nil {
		panic(err)
	}
	perPage := int64(100)
	metadata, pages, err := result(client, id, perPage)
	if err != nil {
		panic(err)
	}
	if *readableOpt {
		err = outputTable(metadata, pages, perPage)
		if err != nil {
			panic(err)
		}
	} else {
		err = output(metadata, pages, perPage)
		if err != nil {
			panic(err)
		}

	}
}

func execute(client *athena.Athena, query string) (id *string, err error) {

	resultConf := &athena.ResultConfiguration{}
	resultConf.SetOutputLocation(os.Getenv("ATHENA_OUTPUT_LOCATION"))

	input := &athena.StartQueryExecutionInput{
		QueryString:         aws.String(query),
		ResultConfiguration: resultConf,
	}

	output, err := client.StartQueryExecution(input)
	if err != nil {
		return nil, err
	}

	id = output.QueryExecutionId
	executionInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: id,
	}

	// クエリの完了を待つ
	for {
		executionOutput, err := client.GetQueryExecution(executionInput)
		if err != nil {
			return nil, err
		}
		switch *executionOutput.QueryExecution.Status.State {
		case athena.QueryExecutionStateQueued, athena.QueryExecutionStateRunning:
			time.Sleep(5 * time.Second)
		case athena.QueryExecutionStateSucceeded:
			return id, nil
		default:
			return nil, errors.New(executionOutput.String())
		}
	}
}

func result(client *athena.Athena, queryExecutionId *string, perPage int64) (*athena.ResultSetMetadata, [][]*athena.Row, error) {
	param := &athena.GetQueryResultsInput{
		MaxResults:       aws.Int64(perPage),
		QueryExecutionId: queryExecutionId,
	}

	var metadata *athena.ResultSetMetadata
	pages := make([][]*athena.Row, 0)
	err := client.GetQueryResultsPages(param, func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
		if metadata == nil {
			metadata = page.ResultSet.ResultSetMetadata
		}
		pages = append(pages, page.ResultSet.Rows)
		if lastPage {
			return false
		} else {
			return true
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return metadata, pages, nil
}

func output(metadata *athena.ResultSetMetadata, pages [][]*athena.Row, perPage int64) error {
	for _, page := range pages {
		for _, row := range page {
			strRow := make([]string, len(row.Data))
			for i, col := range row.Data {
				if col == nil || col.VarCharValue == nil {
					strRow[i] = "null"
				} else {
					strRow[i] = *col.VarCharValue
				}
			}
			fmt.Printf("%s\n", strings.Join(strRow, ","))
		}
	}
	return nil
}

func outputTable(metadata *athena.ResultSetMetadata, pages [][]*athena.Row, perPage int64) error {
	table := tablewriter.NewWriter(os.Stdout)

	header := make([]string, len(metadata.ColumnInfo))
	for i, h := range metadata.ColumnInfo {
		header[i] = *h.Name
	}
	table.SetHeader(header)

	for _, page := range pages {
		for _, row := range page {
			strRow := make([]string, len(row.Data))
			for i, col := range row.Data {
				if col == nil || col.VarCharValue == nil {
					strRow[i] = "null"
				} else {
					strRow[i] = *col.VarCharValue
				}
			}
			table.Append(strRow)
		}
	}
	table.Render()
	return nil
}
