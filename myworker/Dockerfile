# A worker image carrying your own flows.
#
# The build context is the PARENT directory, not this one, because go.mod
# replaces github.com/primex/primeflow with `..`:
#
#   docker buildx build --platform linux/amd64 \
#     -f myworker/Dockerfile -t <registry>/myworker:$(git describe --tags --always) --push .
#
# Once the primeflow module path resolves on its own, drop the replace, and
# this context narrows to `myworker/` alone.
FROM golang:1.25-alpine AS build

WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Dependencies first, so a flow-only change reuses the module cache layer. Both
# go.mod files are needed: the replace directive makes the parent's the source
# of every version in the graph.
COPY go.mod go.sum ./
COPY myworker/go.mod myworker/go.sum ./myworker/
RUN cd myworker && go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev

# CGO off keeps the binary static; trimpath and -s -w keep it small.
RUN cd myworker && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/myworker .

FROM gcr.io/distroless/static-debian12:nonroot

# The base carries CA certificates, which http-healthcheck needs to dial https.
COPY --from=build /out/myworker /usr/local/bin/myworker

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/myworker"]
