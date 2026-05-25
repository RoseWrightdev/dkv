FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o dkv ./cmd/dkv

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/dkv .

RUN mkdir -p /data && chown -R 1000:1000 /data
USER 1000:1000

ENV DKV_WAL_PATH=/data/wal
ENV DKV_SNP_PATH=/data/snapshot.bin

EXPOSE 7946 50051

ENTRYPOINT ["./dkv"]
