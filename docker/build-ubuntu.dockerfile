ARG UBT_ID=24.04
FROM ubuntu:$UBT_ID

# Set environment variable to non-interactive to avoid prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install initial packages, add Toolchain PPA, and ensure GCC/G++ are version 11 or higher
RUN set -ex && \
    # Update package lists
    apt update && \
    # Install software-properties-common to manage PPAs
    apt install -y --no-install-recommends software-properties-common && \
    # Add the Ubuntu Toolchain PPA for newer GCC versions
    add-apt-repository ppa:ubuntu-toolchain-r/test && \
    # Update package lists after adding PPA
    apt update && \
    # Install essential build tools and dependencies
    apt install -y --no-install-recommends \
        cmake \
        make \
        ca-certificates \
        gcc \
        g++ \
        libc6-dev \
        pkg-config \
        libssl-dev \
        wget \
        git \
        curl \
        unzip && \
    # Extract major version numbers of gcc and g++
    GCC_VERSION=$(gcc -dumpversion | cut -d. -f1) && \
    GPP_VERSION=$(g++ -dumpversion | cut -d. -f1) && \
    echo "Installed GCC version: $GCC_VERSION" && \
    echo "Installed G++ version: $GPP_VERSION" && \
    # Check if GCC or G++ versions are less than 11
    if [ "$GCC_VERSION" -lt 11 ] || [ "$GPP_VERSION" -lt 11 ]; then \
        echo "Upgrading GCC and G++ to version 11..." && \
        # Install GCC-11 and G++-11 from the Toolchain PPA
        apt install -y --no-install-recommends gcc-11 g++-11 && \
        # Configure update-alternatives to set GCC-11 as the default gcc
        update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 && \
        update-alternatives --set gcc /usr/bin/gcc-11 && \
        # Configure update-alternatives to set G++-11 as the default g++
        update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100 && \
        update-alternatives --set g++ /usr/bin/g++-11 && \
        # Verify the upgrade
        gcc --version && \
        g++ --version; \
    else \
        echo "GCC and G++ versions are sufficient."; \
    fi && \
    # Verify CMake version and upgrade if necessary
    CMAKE_VERSION=$(cmake --version | head -n1 | awk '{print $3}') && \
    echo "Installed CMake version: $CMAKE_VERSION" && \
    # Check if CMake version is less than 3.21.3
    if dpkg --compare-versions "$CMAKE_VERSION" lt "3.21.3"; then \
        echo "Upgrading CMake to version 3.21.3..." && \
        # Remove the older CMake
        apt-get remove -y cmake && \
        # Download and install CMake 3.21.3
        wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3-linux-x86_64.sh && \
        chmod +x cmake-3.21.3-linux-x86_64.sh && \
        ./cmake-3.21.3-linux-x86_64.sh --prefix=/usr/local --skip-license && \
        rm cmake-3.21.3-linux-x86_64.sh && \
        # Verify the upgrade
        cmake --version; \
    else \
        echo "CMake version is sufficient."; \
    fi && \
    # Clean up to reduce image size
    apt clean && \
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
    cd ../ && \
    rm -rf protobuf