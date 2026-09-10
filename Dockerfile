FROM golang:1.26 AS build
WORKDIR /src
COPY . .
WORKDIR /src/cmd
RUN mkdir -p /out/data && CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/brokerd ./brokerd
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/loadgen ./loadgen
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/native-bench ./native-bench

FROM gcr.io/distroless/static-debian12:nonroot AS brokerd
COPY --from=build --chown=65532:65532 /out/brokerd /brokerd
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8090 8091 1883 5672 50051
USER 65532:65532
ENTRYPOINT ["/brokerd"]

FROM gcr.io/distroless/static-debian12:nonroot AS loadgen
COPY --from=build --chown=65532:65532 /out/loadgen /loadgen
USER 65532:65532
ENTRYPOINT ["/loadgen"]

FROM gcr.io/distroless/static-debian12:nonroot AS native-bench
COPY --from=build --chown=65532:65532 /out/native-bench /native-bench
USER 65532:65532
ENTRYPOINT ["/native-bench"]
