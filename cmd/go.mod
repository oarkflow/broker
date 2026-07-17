module github.com/oarkflow/broker/cmd

go 1.23.2

require (
	github.com/oarkflow/broker v0.0.0
	github.com/oarkflow/broker/contrib v0.0.0
	github.com/oarkflow/zlog v0.0.1
)

replace (
	github.com/oarkflow/broker => ../
	github.com/oarkflow/broker/contrib => ../contrib
)
