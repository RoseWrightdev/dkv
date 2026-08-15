FROM golang:1.26.6-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o oryx ./cmd/oryx

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/oryx .

RUN mkdir -p /data && chown -R 1000:1000 /data
USER 1000:1000

ENV ORYX_WAL_PATH=/data/wal
ENV ORYX_SNP_PATH=/data/snapshot.bin
ENV ORYX_RESP_PORT=6379

EXPOSE 7946 50051 6379

ENTRYPOINT ["./oryx"]
