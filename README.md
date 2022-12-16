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
CREATE OR REPLACE EXTERNAL FUNCTION udf_kpl_deaggregate(varchar(max))
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
    partition_key,
    shard_id,
    sequence_number,
    JSON_PARSE(udf_kpl_deaggregate(from_varbyte(kinesis_data,'hex'))) as kinesis_data,
    refresh_time
FROM kinesis.my_stream_name
WHERE is_valid_json_array(udf_kpl_deaggregate(from_varbyte(kinesis_data,'hex')));
```

```sql
REFRESH MATERIALIZED VIEW my_view;
SELECT approximate_arrival_timestamp,partition_key,shard_id,sequence_number,refresh_time, data
from my_view as record, record.kinesis_data as data
order by approximate_arrival_timestamp desc, sequence_number desc
```

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
