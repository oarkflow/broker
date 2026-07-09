module github.com/oarkflow/broker/examples

go 1.26.2

require (
	github.com/oarkflow/broker v0.0.0
	github.com/oarkflow/broker/contrib v0.0.0
	github.com/oarkflow/zlog v0.0.1
)

replace (
	github.com/oarkflow/broker => ../
	github.com/oarkflow/broker/contrib => ../contrib
)
