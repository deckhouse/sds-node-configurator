FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /workspace

COPY api/ api/
COPY e2e/ e2e/

WORKDIR /workspace/e2e

RUN go mod download
RUN go install github.com/onsi/ginkgo/v2/ginkgo@latest

RUN CGO_ENABLED=0 GOOS=linux go test -c -o /workspace/e2e.test ./tests/

FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /go/bin/ginkgo /usr/local/bin/ginkgo
COPY --from=builder /workspace/e2e.test /usr/local/bin/e2e.test

ENTRYPOINT ["/usr/local/bin/e2e.test", "-test.v", "-ginkgo.v", "-ginkgo.progress"]
