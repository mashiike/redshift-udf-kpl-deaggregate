# redshift-udf-kpl-deaggregate
Lambda UDF to de-aggregate KPL for Redshift

## Usage 


### Deploy Lambda Function

Download binary from [Releases](https://github.com/mashiike/prepalert/releases).
Then create a zip archive like the following and deploy it. (runtime `provided.al2`)

```
lambda.zip
└── bootstrap  # build binary
```

A related document is [https://docs.aws.amazon.com/lambda/latest/dg/runtimes-custom.html](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-custom.html)

deploy lambda function example in [lambda directory](lambda/)  
The example of lambda directory uses [lambroll](https://github.com/fujiwara/lambroll) for deployment.

### Create Redshift UDF

```sql
CREATE OR REPLACE EXTERNAL FUNCTION udf_kpl_deaggregate(varbyte(max))
RETURNS varchar(max)
IMMUTABLE
LAMBDA 'redshift-udf-kpl-deaggregate'
IAM_ROLE 'arn:aws:iam::012345678910:role/lambda-udf-redshift';
```

### use with Redshift Streaming ingestion 

see details: https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-streaming-ingestion.html

```sql
CREATE EXTERNAL SCHEMA kinesis
FROM KINESIS
IAM_ROLE default ;
```

```sql
CREATE MATERIALIZED VIEW my_view AS
SELECT 
    approximate_arrival_timestamp,
    JSON_PARSE(kinesis_data) as kinesis__data
FROM (
    SELECT 
        approximate_arrival_timestamp,
        udf_kpl_deaggregate(kinesis_data) as kinesis_data
    FROM kinesis.my_stream_name
)
WHERE is_utf8(kinesis_data) AND is_valid_json(kinesis_data);
```

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
