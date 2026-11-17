# Runtime image for capi-autoscaler
# Binary is built locally by Tilt/cargo, this just packages it with OR-tools runtime
FROM debian:trixie-slim

# Install OR-Tools runtime libraries and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && curl -L -o ortools.tar.gz https://github.com/google/or-tools/releases/download/v9.14/or-tools_amd64_debian-12_cpp_v9.14.6206.tar.gz \
    && mkdir -p /opt/or-tools/current \
    && tar -xzf ortools.tar.gz -C /opt/or-tools/current --strip-components=1 \
    && rm ortools.tar.gz \
    && echo "/opt/or-tools/current/lib" > /etc/ld.so.conf.d/ortools.conf \
    && ldconfig \
    && apt-get remove -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/opt/or-tools/current/lib

# Create nonroot user
RUN useradd --uid 65532 --create-home --shell /bin/bash nonroot

WORKDIR /
COPY target/release/capi-autoscaler /capi-autoscaler
USER 65532:65532

ENTRYPOINT ["/capi-autoscaler"]
