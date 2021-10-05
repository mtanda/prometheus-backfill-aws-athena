package main

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
)

func Test_parse(t *testing.T) {
	expectedTimestamp := int64(1609459200)
	expectedValue := 123.45
	expectedLabelValue := "label_value1"

	results := athena.GetQueryResultsOutput{
		ResultSet: &athena.ResultSet{
			ResultSetMetadata: &athena.ResultSetMetadata{
				ColumnInfo: []*athena.ColumnInfo{
					{
						Name: aws.String("timestamp"),
					},
					{
						Name: aws.String("value"),
					},
					{
						Name: aws.String("label_name1"),
					},
				},
			},
			Rows: []*athena.Row{
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String("timestamp"),
						},
						{
							VarCharValue: aws.String("value"),
						},
						{
							VarCharValue: aws.String("label_name1"),
						},
					},
				},
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String(strconv.FormatInt(expectedTimestamp, 10)),
						},
						{
							VarCharValue: aws.String(strconv.FormatFloat(expectedValue, 'f', -1, 64)),
						},
						{
							VarCharValue: aws.String(expectedLabelValue),
						},
					},
				},
			},
		},
	}

	m, err := parse(&results)
	if err != nil {
		t.Errorf("")
	}

	timestamp := m[0].Timestamp
	if timestamp != expectedTimestamp {
		t.Errorf("got: %v\nwant: %v", timestamp, expectedTimestamp)
	}
	value := m[0].Value
	if value != expectedValue {
		t.Errorf("got: %v\nwant: %v", value, expectedValue)
	}
	labelValue := m[0].Labels["label_name1"]
	if labelValue != expectedLabelValue {
		t.Errorf("got: %v\nwant: %v", labelValue, expectedLabelValue)
	}
}
