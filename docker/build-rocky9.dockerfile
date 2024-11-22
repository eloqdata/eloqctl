FROM rockylinux:9

RUN set -ex; \
    dnf update -y; \
    dnf install -y dnf-plugins-core; \
    dnf install -y epel-release; \
    dnf config-manager --set-enabled crb; \
    dnf install -y cmake make ca-certificates gcc-c++ glibc-devel pkg-config openssl-devel; \
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

# Compile protobuf from source code.  Protobuf version need be compatibility
# with both brpc and grpc. It cannot be too high or too low.
RUN mkdir -p $HOME/Downloads/protobuf && cd $HOME/Downloads/protobuf && \
    curl -fsSL https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=yes \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_ABSL_PROVIDER=package \
    -S . -B cmake-out && \
    cmake --build cmake-out -- -j ${NCPU:-4} && \
    cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
    ldconfig && \
    cd ../ && rm -rf protobuf
    