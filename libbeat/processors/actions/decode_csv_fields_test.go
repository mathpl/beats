package actions

import (
	"testing"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/stretchr/testify/assert"
)

var header = [3]string{"h1", "h2", "h3"}
var testConfigCSV, _ = common.NewConfigFrom(map[string]interface{}{
	"field":  "msg",
	"header": header,
})

var headerComplex = [23]string{"log_time", "user_name", "database_name", "process_id", "connection_from", "session_id", "session_line_num", "command_tag", "session_start_time", "virtual_transaction_id", "transaction_id", "error_severity", "sql_state_code", "message", "detail", "hint", "internal_query", "internal_query_pos", "context", "query", "query_pos", "location", "application_name"}
var testConfigCSVComplex, _ = common.NewConfigFrom(map[string]interface{}{
	"field":  "msg",
	"header": headerComplex,
})

func TestValidCSVComplex(t *testing.T) {
	input := common.MapStr{
		"msg":      `2017-03-28 03:52:16.076 UTC,,,7547,"20.217.70.4:42146",58d9ddf0.1d7b,1,"",2017-03-28 03:52:16 UTC,,0,LOG,00000,"connection received: host=20.217.70.1 port=42146",,,,,,,,"BackendInitialize, postmaster.c:4145",""`,
		"pipeline": "us1",
	}

	actual := getActualValueCSV(t, testConfigCSVComplex, input)

	expected := common.MapStr{
		"msg": map[string]interface{}{
			"connection_from":        "20.217.70.4:42146",
			"session_start_time":     "2017-03-28 03:52:16 UTC",
			"detail":                 "",
			"internal_query":         "",
			"query":                  "",
			"query_pos":              "",
			"application_name":       "",
			"user_name":              "",
			"process_id":             "7547",
			"command_tag":            "",
			"virtual_transaction_id": "",
			"hint":               "",
			"context":            "",
			"database_name":      "",
			"error_severity":     "LOG",
			"sql_state_code":     "00000",
			"message":            "connection received: host=20.217.70.1 port=42146",
			"internal_query_pos": "",
			"location":           "BackendInitialize, postmaster.c:4145",
			"log_time":           "2017-03-28 03:52:16.076 UTC",
			"session_line_num":   "1",
			"transaction_id":     "0",
			"session_id":         "58d9ddf0.1d7b",
		},
		"pipeline": "us1",
	}
	assert.Equal(t, expected.String(), actual.String())

}

func TestValidCSVQuoted(t *testing.T) {
	input := common.MapStr{
		"msg":      `header1,header2,"header3 ""test"" , other test"`,
		"pipeline": "us1",
	}

	actual := getActualValueCSV(t, testConfigCSV, input)

	expected := common.MapStr{
		"msg": map[string]interface{}{
			"h1": "header1",
			"h2": "header2",
			"h3": `header3 "test" , other test`,
		},
		"pipeline": "us1",
	}
	assert.Equal(t, expected.String(), actual.String())

}

func TestInvalidCSVLengthMismatch(t *testing.T) {
	input := common.MapStr{
		"msg":      "header1,header2",
		"pipeline": "us1",
	}

	actual := getActualValueCSV(t, testConfigCSV, input)

	expected := common.MapStr{
		"msg":      "header1,header2",
		"pipeline": "us1",
	}

	assert.Equal(t, expected.String(), actual.String())

}

func TestValidCSV(t *testing.T) {
	input := common.MapStr{
		"msg":      "header1,header2,header3",
		"pipeline": "us1",
	}

	actual := getActualValueCSV(t, testConfigCSV, input)

	expected := common.MapStr{
		"msg": map[string]interface{}{
			"h1": "header1",
			"h2": "header2",
			"h3": "header3",
		},
		"pipeline": "us1",
	}

	assert.Equal(t, expected.String(), actual.String())

}

func getActualValueCSV(t *testing.T, config *common.Config, input common.MapStr) common.MapStr {
	if testing.Verbose() {
		logp.LogInit(logp.LOG_DEBUG, "", false, true, []string{"*"})
	}

	p, err := newDecodeCSVFields(*config)
	if err != nil {
		logp.Err("Error initializing decode_csv_fields")
		t.Fatal(err)
	}

	actual, err := p.Run(input)

	return actual
}
