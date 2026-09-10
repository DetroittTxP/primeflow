# Multi-stage build. The final image is distroless-style: a static binary and
# nothing else, so there is no shell or package manager to attack.
FROM golang:1.25-alpine AS build

WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Dependencies first, so a code-only change reuses the module cache layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Redeclared without defaults on purpose: BuildKit fills these from --platform,
# but only for an ARG with no value of its own. A default here wins instead,
# which pins GOARCH to amd64 and yields an amd64 binary inside an
# arm64-labelled image — the exact mismatch docker-push exists to avoid.
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

# CGO off keeps the binary static; trimpath and -s -w keep it small.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/primeflow ./cmd/primeflow && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" \
    -o /out/primex-worker ./examples/primex-worker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/primeflow /usr/local/bin/primeflow
COPY --from=build /out/primex-worker /usr/local/bin/primex-worker

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/primeflow"]
CMD ["server"]
