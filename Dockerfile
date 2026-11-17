FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine AS build
ARG TARGETOS TARGETARCH TARGETVARIANT VERSION=dev
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /sakurako .

FROM scratch
# HTTPS のバックエンドに繋ぐため、ビルド側の CA 証明書を持ち込む
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /sakurako /sakurako
USER 65532:65532
EXPOSE 40772
ENTRYPOINT ["/sakurako"]
CMD ["-config", "/etc/sakurako/sakurako.json"]
