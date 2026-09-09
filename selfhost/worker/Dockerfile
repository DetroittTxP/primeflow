# A worker image carrying your own flows.
#
# The build context is this directory alone -- go.mod depends on PrimeFlow
# through the module proxy, so nothing outside it is needed:
#
#   docker build -t my-worker:dev .
#
# For a registry, name the platform. TARGETARCH defaults to amd64, so a plain
# build on an arm64 machine produces an amd64 binary inside an arm64-*labelled*
# image -- it runs, under emulation, and the mislabelling is confusing later.
#
#   docker buildx build --platform linux/amd64 \
#     -t <registry>/my-worker:$(git describe --tags --always) --push .
FROM golang:1.25-alpine AS build

WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Dependencies first, so a flow-only change reuses the module cache layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev

# CGO off keeps the binary static; trimpath and -s -w keep it small.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/worker .

# Distroless: a static binary and nothing else, so there is no shell or package
# manager in the image to attack. The base carries CA certificates, which a
# flow calling an https API needs.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/worker /usr/local/bin/worker

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/worker"]
