FROM golang:1.25-alpine AS build
WORKDIR /src

# Download dependencies first so they cache independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# .dockerignore strips .git, so build info is passed in rather than derived.
ARG TAG=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

# GOEXPERIMENT=jsonv2 is required by mautrix's federation/pdu package
# (per-room-version redaction, hashing, signing, event IDs). It also switches
# encoding/json to the v2 backend process-wide; every ported test suite runs
# under it in CI.
RUN CGO_ENABLED=0 GOEXPERIMENT=jsonv2 go build -trimpath \
      -ldflags="-s -w \
        -X main.tag=${TAG} \
        -X main.commit=${COMMIT} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/goworker ./cmd/goworker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/goworker /goworker
ENTRYPOINT ["/goworker"]
CMD ["-config", "/data/goworker.yaml"]
