package helpers

import "github.com/aws/aws-sdk-go/service/timestreamquery"

func ProcessScalarType(datum *timestreamquery.Datum) string {
	if datum.NullValue != nil && *datum.NullValue {
		return "NULL"
	}
	return *datum.ScalarValue
}

func ProcessTimeSeriesType(data []*timestreamquery.TimeSeriesDataPoint, columnInfo *timestreamquery.ColumnInfo) map[string]interface{} {
	m := make(map[string]interface{})
	for k := 0; k < len(data); k++ {
		time := data[k].Time
		if columnInfo.Type.ScalarType != nil {
			m[*time] = ProcessScalarType(data[k].Value)
		} else if columnInfo.Type.ArrayColumnInfo != nil {
			m[*time] = ProcessArrayType(data[k].Value.ArrayValue, columnInfo.Type.ArrayColumnInfo)
		} else if columnInfo.Type.RowColumnInfo != nil {
			m[*time] = ProcessRowType(data[k].Value.RowValue.Data, columnInfo.Type.RowColumnInfo)
		}
	}
	return m
}

func ProcessArrayType(datumList []*timestreamquery.Datum, columnInfo *timestreamquery.ColumnInfo) []interface{} {
	var s []interface{}
	for k := 0; k < len(datumList); k++ {
		if columnInfo.Type.ScalarType != nil {
			s = append(s, ProcessScalarType(datumList[k]))
		} else if columnInfo.Type.TimeSeriesMeasureValueColumnInfo != nil {
			s = append(s, ProcessTimeSeriesType(datumList[k].TimeSeriesValue, columnInfo.Type.TimeSeriesMeasureValueColumnInfo))
		} else if columnInfo.Type.ArrayColumnInfo != nil {
			s = append(s, ProcessArrayType(datumList[k].ArrayValue, columnInfo.Type.ArrayColumnInfo))
		} else if columnInfo.Type.RowColumnInfo != nil {
			s = append(s, ProcessRowType(datumList[k].RowValue.Data, columnInfo.Type.RowColumnInfo))
		}
	}
	return s
}

func ProcessRowType(data []*timestreamquery.Datum, metadata []*timestreamquery.ColumnInfo) map[string]interface{} {
	m := make(map[string]interface{})
	for j := 0; j < len(data); j++ {
		columnName := *metadata[j].Name
		if metadata[j].Type.ScalarType != nil {
			m[columnName] = ProcessScalarType(data[j])
		} else if metadata[j].Type.TimeSeriesMeasureValueColumnInfo != nil {
			datapointList := data[j].TimeSeriesValue
			m[columnName] = ProcessTimeSeriesType(datapointList, metadata[j].Type.TimeSeriesMeasureValueColumnInfo)
		} else if metadata[j].Type.ArrayColumnInfo != nil {
			columnInfo := metadata[j].Type.ArrayColumnInfo
			datumList := data[j].ArrayValue
			m[columnName] = ProcessArrayType(datumList, columnInfo)
		} else if metadata[j].Type.RowColumnInfo != nil {
			columnInfo := metadata[j].Type.RowColumnInfo
			datumList := data[j].RowValue.Data
			m[columnName] = ProcessRowType(datumList, columnInfo)
		}
	}
	return m
}
