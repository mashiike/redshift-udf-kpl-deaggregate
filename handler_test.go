package redshiftudfkpldeaggregate

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}

func TestRowHandlerFunc(t *testing.T) {
	cases := []struct {
		casename string
		input    []interface{}
		errStr   string
		output   interface{}
	}{
		{
			casename: "not kpl aggregated record and no json",
			input: []interface{}{
				[]byte(`this is data`),
			},
			output: []json.RawMessage{
				json.RawMessage(`"this is data"`),
			},
		},
		{
			casename: "not kpl aggregated record and json",
			input: []interface{}{
				[]byte(`{"hoge":1}`),
			},
			output: []json.RawMessage{
				json.RawMessage(`{"hoge":1}`),
			},
		},
		{
			casename: "kpl aggregated record",
			input: []interface{}{
				must(hex.DecodeString("f3899ac20a01610a2033346239346331653233373332376365663764313265653830366238623238361a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a1a2408011a207b2264617461223a22746869732069732070696e67206d657373616765227d0a8ed8d0c14b003c2d35769582feb8b334")),
			},
			output: lo.RepeatBy(13, func(_ int) json.RawMessage {
				return json.RawMessage(`{"data":"this is ping message"}`)
			}),
		},
		{
			casename: "not kpl aggregated record and json part2",
			input: []interface{}{
				must(hex.DecodeString("7b22686f6765223a22686f6765222c20226964223a317d")),
			},
			output: lo.RepeatBy(1, func(_ int) json.RawMessage {
				return json.RawMessage(`{"hoge":"hoge", "id":1}`)
			}),
		},
		{
			casename: "kpl aggregated record and no json",
			input: []interface{}{
				must(hex.DecodeString("f3899ac20a03666f6f0a036261721203666f6f12036261721a0b080010011a0564617461311a0b080010011a0564617461326338f174dbbf14506cacdddc9314ee37")),
			},
			output: []json.RawMessage{
				json.RawMessage(`"data1"`),
				json.RawMessage(`"data2"`),
			},
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case.%d_%s", i+1, c.casename), func(t *testing.T) {
			output, err := RowHandlerFunc(context.Background(), c.input)
			if c.errStr == "" {
				require.NoError(t, err)
				require.EqualValues(t, c.output, output)
			} else {
				require.EqualError(t, err, c.errStr)
			}
		})
	}
}
