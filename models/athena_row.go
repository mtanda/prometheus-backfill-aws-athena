package models

type AthenaRow struct {
	Timestamp int64
	Value     float64 `prometheus:"metric_type:gauge"`
	Labels    map[string]string
}

func (r AthenaRow) GetAdditionalLabels() map[string]string {
	return r.Labels
}
