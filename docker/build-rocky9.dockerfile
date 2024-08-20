FROM rockylinux:9

RUN set -ex; \
    dnf update -y; \
    dnf install -y dnf-plugins-core; \
    dnf install -y epel-release; \
    dnf config-manager --set-enabled crb; \
    dnf install -y ca-certificates gcc glibc-devel pkg-config openssl-devel; \
    dnf install -y wget git unzip; \
    dnf clean all; 

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