package redshiftudfkpldeaggregate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/fujiwara/kinesis-tailf/kpl"
)

func RowHandlerFunc(ctx context.Context, args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("udf_kpl_deaggregate takes 1 argument: %d arguments are received. ", len(args))
	}
	bs, ok := args[0].([]byte)
	if !ok {
		return nil, errors.New("the first argument of udf_kpl_deaggregate must be interpreted as a byte")
	}
	log.Printf("[debug] udf_kpl_deaggregate([]byte{%x})", bs)
	records := make([]json.RawMessage, 0)
	ar, err := kpl.Unmarshal(bs)
	if err != nil {
		log.Printf("[debug] can not unmarshal KPL: %v", err)
		records = append(records, convertJSON(bs))
		return records, nil
	}
	for _, r := range ar.Records {
		records = append(records, convertJSON(r.Data))
	}
	return records, nil
}

func convertJSON(bs []byte) json.RawMessage {
	var js json.RawMessage
	err := json.Unmarshal(bs, &js)
	if err == nil {
		return js
	}
	ret, _ := json.Marshal(string(bs))
	return ret
}
