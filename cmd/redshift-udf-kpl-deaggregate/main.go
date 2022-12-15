package main

import (
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/hashicorp/logutils"
	"github.com/mashiike/gravita"
	redshiftudfkpldeaggregate "github.com/mashiike/redshift-udf-kpl-deaggregate"
)

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "notice", "warn", "error"},
		MinLevel: logutils.LogLevel(logLevel),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	mux := gravita.NewMux()
	mux.HandleRowFunc("*", redshiftudfkpldeaggregate.RowHandlerFunc)
	log.Println("[info] start lambda handler")
	lambda.Start(mux.HandleLambdaEvent)
	log.Println("[info] end lambda handler")
}
