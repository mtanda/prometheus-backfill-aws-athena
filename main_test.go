package main

import (
	"reflect"
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

	ch := make(chan interface{}, 1)
	parse(&results, ch)
	mm := <-ch

	m := reflect.ValueOf(mm).Elem()
	timestamp := m.FieldByName("Timestamp").Int()
	if timestamp != expectedTimestamp {
		t.Errorf("got: %v\nwant: %v", timestamp, expectedTimestamp)
	}
	value := m.FieldByName("Value").Float()
	if value != expectedValue {
		t.Errorf("got: %v\nwant: %v", value, expectedValue)
	}
	labelValue := m.FieldByName("Labels").MapIndex(reflect.ValueOf("label_name1")).String()
	if labelValue != expectedLabelValue {
		t.Errorf("got: %v\nwant: %v", labelValue, expectedLabelValue)
	}
}
