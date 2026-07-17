FROM golang:1.26 AS build
WORKDIR /src
COPY . .
WORKDIR /src/cmd
RUN CGO_ENABLED=0 go build -o /out/brokerd ./brokerd
RUN CGO_ENABLED=0 go build -o /out/loadgen ./loadgen

FROM gcr.io/distroless/static-debian12 AS brokerd
COPY --from=build /out/brokerd /brokerd
EXPOSE 8090
ENTRYPOINT ["/brokerd"]

FROM gcr.io/distroless/static-debian12 AS loadgen
COPY --from=build /out/loadgen /loadgen
ENTRYPOINT ["/loadgen"]
