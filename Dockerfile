# syntax=docker/dockerfile:1.7

# ---- build ----------------------------------------------------------------
# Build on the runner's native platform and cross-compile for the target:
# much faster than emulating arm64 with QEMU.
# Keep this tag in step with the "go" line in go.mod.
FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine AS build
WORKDIR /src

# Dependencies first: this layer is cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/webevent-server ./cmd/webevent-server && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/webevent ./cmd/webevent

# ---- runtime --------------------------------------------------------------
# distroless/static: no shell, no package manager, CA certificates included,
# runs as an unprivileged user. Tiny attack surface.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/webevent-server /usr/local/bin/webevent-server
COPY --from=build /out/webevent /usr/local/bin/webevent
# Numeric ID: Kubernetes cannot verify a named user is non-root (runAsNonRoot).
USER 65532:65532
EXPOSE 8080 9090
HEALTHCHECK --interval=10s --timeout=4s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/webevent-server", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/webevent-server"]
CMD ["serve"]
