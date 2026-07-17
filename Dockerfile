FROM golang:1.23 AS build
WORKDIR /src
COPY . .
WORKDIR /src/cmd
RUN CGO_ENABLED=0 go build -o /out/brokerd ./brokerd
RUN CGO_ENABLED=0 go build -o /out/loadgen ./loadgen
RUN CGO_ENABLED=0 go build -o /out/native-bench ./native-bench

FROM gcr.io/distroless/static-debian12 AS brokerd
COPY --from=build /out/brokerd /brokerd
EXPOSE 8090 8091
ENTRYPOINT ["/brokerd"]

FROM gcr.io/distroless/static-debian12 AS loadgen
COPY --from=build /out/loadgen /loadgen
ENTRYPOINT ["/loadgen"]

FROM gcr.io/distroless/static-debian12 AS native-bench
COPY --from=build /out/native-bench /native-bench
ENTRYPOINT ["/native-bench"]
