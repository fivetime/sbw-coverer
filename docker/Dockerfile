# syntax=docker/dockerfile:1.7
###############################################################################
# sbw-coverer — the SBW control-plane SHARDED sensor/actuator: the embedded GoBGP
# RIB tap + RIB-survival guard + the agent-facing AgentService gRPC. It DIALS the
# sbw-server (rpc.ServerCoverer) and owns NO store (no etcd, no YugabyteDB). Pure
# Go, NO cgo → a tiny static binary on distroless.
#
# go.mod has TWO path replaces, so the builder clones the sibling repos:
#   replace github.com/fivetime/sbw-contract => ../sbw-contract
#   replace github.com/osrg/gobgp/v4        => ../gobgp   (L-01 multihop-BFD fork)
# Refs are build-args; CI passes the triggering SHA. For private repos pass GH_TOKEN.
###############################################################################
FROM golang:1.25-alpine AS builder

ARG COVERER_REPO=github.com/fivetime/sbw-coverer
ARG COVERER_REF=main
ARG CONTRACT_REPO=github.com/fivetime/sbw-contract
ARG CONTRACT_REF=main
ARG GOBGP_REPO=github.com/fivetime/gobgp
ARG GOBGP_REF=master
ARG GH_TOKEN=""
ARG VERSION=dev
# CACHEBUST (the triggering commit SHA, passed as --build-arg CACHEBUST=${{ github.sha }}
# from CI) is referenced in the clone RUN below so that RUN's cache key changes every
# commit. Without it buildx caches the `git clone -b main` layer (the command string is
# constant) and every build re-serves the FIRST clone's STALE source — producing an
# identical, perpetually-old image no matter what is pushed.
ARG CACHEBUST=dev

RUN apk add --no-cache git ca-certificates
WORKDIR /src
RUN set -eux; \
    : "cachebust ${CACHEBUST}"; \
    auth=""; [ -n "$GH_TOKEN" ] && auth="x-access-token:${GH_TOKEN}@"; \
    git clone --depth 1 -b "$COVERER_REF"  "https://${auth}${COVERER_REPO}.git"  sbw-coverer; \
    git clone --depth 1 -b "$CONTRACT_REF" "https://${auth}${CONTRACT_REPO}.git" sbw-contract; \
    git clone --depth 1 -b "$GOBGP_REF"    "https://${auth}${GOBGP_REPO}.git"    gobgp

WORKDIR /src/sbw-coverer
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/sbw-coverer ./cmd/sbw-coverer

###############################################################################
# runtime — distroless static (no shell, nonroot). The coverer needs the binary +
# CA certs (already in the base) + a config file mounted at runtime.
###############################################################################
FROM gcr.io/distroless/static:nonroot
ARG VERSION=dev
LABEL org.opencontainers.image.title="sbw-coverer" \
      org.opencontainers.image.description="Shared-bandwidth-pool coverer (sharded GoBGP RIB tap + guard + agent gRPC; dials ServerCoverer)" \
      org.opencontainers.image.source="https://github.com/fivetime/sbw-coverer" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=builder /out/sbw-coverer /usr/local/bin/sbw-coverer
# agent gRPC (L/R agents dial) 1791 · GoBGP RIB tap (BGP) 1790 · metrics 9102
EXPOSE 1791 1790 9102
ENTRYPOINT ["/usr/local/bin/sbw-coverer"]
CMD ["-config", "/etc/sbw/coverer.json"]
