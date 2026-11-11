FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /raftkv ./cmd/raftkv

FROM alpine:3.22
COPY --from=build /raftkv /usr/local/bin/raftkv
VOLUME /data
EXPOSE 7000
HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://127.0.0.1:7000/status >/dev/null || exit 1
ENTRYPOINT ["raftkv"]
