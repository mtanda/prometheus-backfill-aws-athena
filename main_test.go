package main

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/mtanda/prometheus-backfill-aws-athena/models"
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

	ch := make(chan *models.AthenaRow, 1)
	parse(&results, ch)
	m := <-ch

	if m.Timestamp != expectedTimestamp {
		t.Errorf("got: %v\nwant: %v", m.Timestamp, expectedTimestamp)
	}
	if m.Value != expectedValue {
		t.Errorf("got: %v\nwant: %v", m.Value, expectedValue)
	}
	if m.Labels["label_name1"] != expectedLabelValue {
		t.Errorf("got: %v\nwant: %v", m.Labels["label_name1"], expectedLabelValue)
	}
}
