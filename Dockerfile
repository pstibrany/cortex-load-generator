FROM golang:1.19 AS mod
WORKDIR $GOPATH/cortex-load-generator
COPY go.mod .
COPY go.sum .
RUN GO111MODULE=on go mod download

FROM golang:1.19 as build
COPY --from=mod $GOCACHE $GOCACHE
COPY --from=mod $GOPATH/pkg/mod $GOPATH/pkg/mod
WORKDIR $GOPATH/cortex-load-generator
COPY . .
RUN rm -fr ./vendor
RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o=/bin/cortex-load-generator ./cmd

FROM scratch
COPY --from=build /bin/cortex-load-generator /bin/cortex-load-generator
ENTRYPOINT ["/bin/cortex-load-generator"]
