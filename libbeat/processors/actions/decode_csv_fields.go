package actions

import (
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/jsontransform"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/pkg/errors"
)

type decodeCSVField struct {
	field         string
	header        []string
	overwriteKeys bool
	target        *string
}

type configCSV struct {
	Field         string   `config:"field"`
	Header        []string `config:"header"`
	OverwriteKeys bool     `config:"overwrite_keys"`
	Target        *string  `config:"target"`
}

var (
	defaultConfigCSV = configCSV{}
)

func init() {
	processors.RegisterPlugin("decode_csv_fields",
		configChecked(newDecodeCSVFields,
			requireFields("field"),
			allowedFields("field", "header", "overwrite_keys", "target", "when")))
}

func newDecodeCSVFields(c common.Config) (processors.Processor, error) {
	config := defaultConfigCSV

	err := c.Unpack(&config)

	if err != nil {
		logp.Warn("Error unpacking config for decode_csv_field")
		return nil, fmt.Errorf("fail to unpack the decode_csv_field configuration: %s", err)
	}

	f := decodeCSVField{field: config.Field, header: config.Header, overwriteKeys: config.OverwriteKeys, target: config.Target}
	return f, nil
}

func (f decodeCSVField) Run(event common.MapStr) (common.MapStr, error) {
	data, err := event.GetValue(f.field)
	if err != nil && errors.Cause(err) != common.ErrKeyNotFound {
		debug("Error trying to GetValue for field : %s in event : %v", f.field, event)
		return event, err

	}

	text, ok := data.(string)
	if ok {
		output, err := DecodeCSV([]byte(text), f.header)
		if err != nil {
			debug("Error trying to decode %s", event[f.field])
			return event, err
		}

		if f.target != nil {
			if len(*f.target) > 0 {
				_, err = event.Put(*f.target, output)
			} else {
				jsontransform.WriteJSONKeys(event, output, f.overwriteKeys, "csv_error")
			}
		} else {
			_, err = event.Put(f.field, output)
		}

		if err != nil {
			debug("Error trying to Put value %v for field : %s", output, f.field)
			return event, err
		}
	}

	return event, nil
}

func DecodeCSV(text []byte, header []string) (map[string]interface{}, error) {
	dec := csv.NewReader(bytes.NewReader(text))

	values, err := dec.Read()
	if err != nil {
		return nil, err
	}

	valuesCount := len(values)
	headerCount := len(header)
	if headerCount != valuesCount {
		return nil, fmt.Errorf("Expected %d csv fields, got %d", headerCount, valuesCount)
	}

	to := make(map[string]interface{}, headerCount)
	for j := range values {
		to[header[j]] = &values[j]
	}

	return to, nil
}

func (f decodeCSVField) String() string {
	return "decode_csv_field=" + f.field
}
