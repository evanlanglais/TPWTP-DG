FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /collector ./cmd/collector
RUN CGO_ENABLED=0 go build -o /backfill ./cmd/backfill

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /collector /collector
COPY --from=build /backfill /backfill

# Default is the long-running collector. The one-off legacy backfill is run
# on demand via the `backfill` compose service (see docker-compose.yml).
ENTRYPOINT ["/collector"]
