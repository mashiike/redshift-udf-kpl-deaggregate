package redshiftudfkpldeaggregate

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"

	"github.com/fujiwara/kinesis-tailf/kpl"
)

func RowHandlerFunc(ctx context.Context, args []interface{}) (interface{}, error) {
	records, err := rowHandlerFunc(ctx, args)
	for err != nil {
		return nil, err
	}
	bs, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	return string(bs), nil
}

func rowHandlerFunc(ctx context.Context, args []interface{}) ([]json.RawMessage, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("udf_kpl_deaggregate takes 1 argument: %d arguments are received.", len(args))
	}
	hexStr, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("1st argument of udf_kpl_deaggregate must be interpreted as a hex string: got %T", args[0])
	}
	log.Printf("[debug] udf_kpl_deaggregate(%s)", hexStr)
	records := make([]json.RawMessage, 0)
	bs, err := hex.DecodeString(hexStr)
	if err != nil {
		log.Printf("[debug] argument is not hex string: %v", err)
		records = append(records, convertJSON([]byte(hexStr)))
		return records, nil
	}
	log.Println("[debug] success decode hex string")
	ar, err := kpl.Unmarshal(bs)
	if err != nil {
		log.Printf("[debug] can not unmarshal KPL: %v", err)
		records = append(records, convertJSON(bs))
		return records, nil
	}
	log.Println("[debug] success KPL deaggregate")
	for i, r := range ar.Records {
		log.Printf("[debug] aggregated recode %d: %x", i, r.Data)
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
