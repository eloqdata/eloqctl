FROM rockylinux:9

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.80.0

RUN set -ex; \
    dnf update -y; \
    dnf install -y dnf-plugins-core; \
    dnf install -y epel-release; \
    dnf config-manager --set-enabled crb; \
    dnf install -y ca-certificates gcc glibc-devel pkg-config openssl-devel; \
    dnf install -y wget git unzip; \
    dnf clean all; \
    # install aws cli
    curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip"; \
    unzip awscliv2.zip && rm awscliv2.zip; \
    ./aws/install && rm -r aws; \
    # install rust
    dpkgArch="$(uname -m)"; \
    case "${dpkgArch}" in \
    amd64 | x86_64) rustArch='x86_64-unknown-linux-gnu'; rustupSha256='0b2f6c8f85a3d02fde2efc0ced4657869d73fccfce59defb4e8d29233116e6db' ;; \
    arm64 | aarch64) rustArch='aarch64-unknown-linux-gnu'; rustupSha256='673e336c81c65e6b16dcdede33f4cc9ed0f08bde1dbe7a935f113605292dc800' ;; \
    *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    url="https://static.rust-lang.org/rustup/archive/1.26.0/${rustArch}/rustup-init"; \
    wget "$url"; \
    echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_VERSION --default-host ${rustArch}; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version; \
    # install cargo make
    cargo install --force cargo-make; 
