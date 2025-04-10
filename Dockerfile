FROM golang:1.24 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/go-events-consumer
COPY . /src/go-events-consumer

RUN go mod tidy
WORKDIR /src/go-events-consumer
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/go-events-consumer/go-events-consumer /app/go-events-consumer

RUN chmod +x /app/go-events-consumer

WORKDIR /app

ENTRYPOINT ["./go-events-consumer"]