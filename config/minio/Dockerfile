FROM golang:1.24.3-alpine AS builder
RUN apk add --no-cache git build-base
RUN git clone --depth 1 --branch RELEASE.2025-04-22T22-12-26Z https://github.com/minio/minio.git /src/minio
WORKDIR /src/minio
RUN CGO_ENABLED=0 go build -trimpath -o /out/minio .
RUN git clone --depth 1 --branch RELEASE.2025-04-16T18-13-26Z https://github.com/minio/mc.git /src/mc
WORKDIR /src/mc
RUN CGO_ENABLED=0 go build -trimpath -o /out/mc .
FROM alpine:3.21.3
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /out/minio /out/mc /usr/local/bin/
ENTRYPOINT ["minio"]
