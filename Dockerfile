# The binary is static and the final image carries nothing but it and the CA
# bundle: there is no shell, no package manager and no libc for anything that
# gets in to use.
FROM golang:1.26-alpine AS build

WORKDIR /src

# The module files are copied first so that the dependency download is cached
# independently of the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

ENV CGO_ENABLED=0

RUN go build -trimpath \
    -ldflags "-s -w \
        -X 'main.version=${VERSION}' \
        -X 'main.commit=${COMMIT}' \
        -X 'main.date=${BUILD_DATE}'" \
    -o /out/outbox ./cmd/outbox

FROM alpine:3 AS certs
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
