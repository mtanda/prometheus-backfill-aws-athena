package main

import (
	"strconv"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/mtanda/prometheus-backfill-aws-athena/models"
)

func parse(results *athena.GetQueryResultsOutput, ch chan *models.AthenaRow) {
	columnInfo := results.ResultSet.ResultSetMetadata.ColumnInfo
	for _, row := range results.ResultSet.Rows {
		m := models.AthenaRow{}
		m.Labels = make(map[string]string)
		for columnIdx, cell := range row.Data {
			columnName := columnInfo[columnIdx].Name
			switch *columnName {
			case "timestamp":
				if cval, err := strconv.ParseInt(*cell.VarCharValue, 10, 64); err != nil {
				} else {
					m.Timestamp = cval
				}
			case "value":
				if cval, err := strconv.ParseFloat(*cell.VarCharValue, 64); err != nil {
				} else {
					m.Value = cval
				}
			default:
				m.Labels[*columnName] = *cell.VarCharValue
			}
		}
		ch <- &m
	}
}
