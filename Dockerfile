FROM --platform=$BUILDPLATFORM golang:1.26.11@sha256:6ef03c9474cb3b3ba3f18e31aa2938ee419e58f164ce5e012b28e61c039b1aa6 AS build
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
