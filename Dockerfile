FROM golang:alpine AS builder

WORKDIR /rclone/
COPY go.mod /rclone/
RUN go mod download

COPY . /rclone/

RUN apk add --no-cache make bash gawk git
RUN \
  CGO_ENABLED=0 \
  make && \
  ./rclone version


# Begin final image
FROM alpine:latest

COPY --from=builder /rclone/rclone /usr/local/bin/

COPY ["./docker/startup", "/startup"]

RUN apk --no-cache add \
  ca-certificates \
  fuse3 \
  tzdata \
  && apk cache clean

RUN echo "user_allow_other" >> /etc/fuse.conf && \
  addgroup -g 1009 rclone && \
  adduser -u 1009 -Ds /bin/sh -G rclone rclone && \
  chmod +x /startup

WORKDIR /data

ENV XDG_CONFIG_HOME=/config

# remote configs
ENV REMOTE_NAME=""
ENV REMOTE_URL=""
ENV REMOTE_VENDOR=""

# s3 proxy configs
# a space separated list of options
ENV PROXY_ARGS=""

ENTRYPOINT [ "/startup" ]

EXPOSE 8080
