FROM rclone/rclone:latest AS binaries

# final image
FROM alpine:latest

# copy the rclone binary from the official image
COPY --from=binaries /usr/local/bin/rclone /usr/local/bin/rclone

# copy startup and auth proxy scripts
COPY ["./docker/startup", "/startup"]
COPY ["./docker/auth-proxy.py", "/usr/local/bin/auth-proxy.py"]

# add python for the auth proxy script
RUN apk --no-cache add \
  python3 \
  ca-certificates \
  fuse3 \
  tzdata \
  && apk cache clean

# make scripts executable
RUN echo "user_allow_other" >> /etc/fuse.conf && \
  chmod +x /startup /usr/local/bin/auth-proxy.py

WORKDIR /data

ENV XDG_CONFIG_HOME=/config

# remote configs (used by auth-proxy.py)
ENV REMOTE_URL=""
ENV REMOTE_VENDOR=""

# s3 proxy configs
# a space separated list of options
ENV PROXY_ARGS=""

ENTRYPOINT [ "/startup" ]

EXPOSE 8080
