# The binary is static and the final image carries nothing but it and the CA
# bundle: there is no shell, no package manager and no libc for anything that
# gets in to use.
# --platform=$BUILDPLATFORM pins the builder to the runner's own architecture,
# and the Go build cross-compiles from there. Without it, buildx would emulate
# the target through QEMU to run a native compile — minutes instead of seconds,
# for a toolchain that cross-compiles perfectly well on its own.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

WORKDIR /src

# The module files are copied first so that the dependency download is cached
# independently of the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

# Supplied by buildx for every platform in --platform.
ARG TARGETOS
ARG TARGETARCH

ENV CGO_ENABLED=0

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w \
        -X 'main.version=${VERSION}' \
        -X 'main.commit=${COMMIT}' \
        -X 'main.date=${BUILD_DATE}'" \
    -o /out/outbox ./cmd/outbox

# Also pinned to the build platform: a CA bundle is the same bytes everywhere,
# and building this stage for the target would drag QEMU into a multi-platform
# build that otherwise needs none.
FROM --platform=$BUILDPLATFORM alpine:3 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/outbox /outbox

# The migrations travel in the image so an operator can apply them with an
# external tool, or from a job that is not this process.
COPY migrations /migrations

# Nobody in particular, and certainly not root.
USER 65534:65534

EXPOSE 8085 9100

ENTRYPOINT ["/outbox"]
CMD ["run"]
