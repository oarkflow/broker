FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:5f1b98fa02fe396ebce2a0227a0e08309e11792915b9dd2e1f71053d7e7e6666 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY . .
WORKDIR /src/cmd
RUN mkdir -p /out/data && CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -trimpath -buildvcs=false -o /out/brokerd ./brokerd
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -trimpath -buildvcs=false -o /out/loadgen ./loadgen
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -trimpath -buildvcs=false -o /out/native-bench ./native-bench

FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS brokerd
COPY --from=build --chown=65532:65532 /out/brokerd /brokerd
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8090 8091 1883 5672 50051
USER 65532:65532
ENTRYPOINT ["/brokerd"]

FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS loadgen
COPY --from=build --chown=65532:65532 /out/loadgen /loadgen
USER 65532:65532
ENTRYPOINT ["/loadgen"]

FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS native-bench
COPY --from=build --chown=65532:65532 /out/native-bench /native-bench
USER 65532:65532
ENTRYPOINT ["/native-bench"]
