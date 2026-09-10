module github.com/oarkflow/broker/examples/pipeline_consumer

go 1.26.5

require (
	github.com/oarkflow/broker v0.0.0
	github.com/oarkflow/dagflow v0.0.0-20260901133735-25152ce6960c
	github.com/oarkflow/zlog v0.0.2
)

replace github.com/oarkflow/broker => ../..
