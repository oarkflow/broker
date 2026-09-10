# Private dagflow integration example

This optional example is an isolated nested module because it depends on the
private `github.com/oarkflow/dagflow` module. Default public example builds do
not enter this module.

To run it, provide dagflow through your own authenticated module configuration
or a `go.work` workspace, then run:

```sh
cd examples/pipeline_consumer
go run .
```

The public broker release and CI do not rely on this private dependency. This
optional module cannot pass the public-build gate until dagflow is published.
