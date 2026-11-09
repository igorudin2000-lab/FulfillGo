FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/order-service ./cmd/order-service
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fulfillment-worker ./cmd/fulfillment-worker

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/order-service /order-service
COPY --from=builder /out/fulfillment-worker /fulfillment-worker
USER 65532:65532
EXPOSE 8080 8081
ENTRYPOINT ["/order-service"]

