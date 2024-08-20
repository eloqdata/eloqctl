ARG UBT_ID=24.04
FROM ubuntu:$UBT_ID

RUN set -ex; \
    apt update; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt install -y --no-install-recommends ca-certificates gcc libc6-dev pkg-config libssl-dev; \
    apt install -y --no-install-recommends wget git curl unzip; \
    rm -rf /var/lib/apt/lists/*

# install aws cli
RUN set -ex; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip"; \
    unzip awscliv2.zip && rm awscliv2.zip; \
    ./aws/install && rm -r aws

# install rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
# install cargo make
RUN cargo install cargo-make
