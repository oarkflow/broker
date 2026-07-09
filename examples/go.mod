module github.com/oarkflow/fastworker/examples

go 1.26.2

require (
	github.com/oarkflow/fastworker v0.0.0
	github.com/oarkflow/fastworker/contrib v0.0.0
	github.com/oarkflow/zlog v0.0.1
)

replace (
	github.com/oarkflow/fastworker => ../
	github.com/oarkflow/fastworker/contrib => ../contrib
)
