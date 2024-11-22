FROM centos:7

RUN set -ex; \
    # https://serverfault.com/questions/1161816/mirrorlist-centos-org-no-longer-resolve
    sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo; \
    sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo; \
    sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo; \
    yum update -y; \
    yum install -y epel-release; \
    yum install -y scl-utils gcc gcc-c++; \
    yum install -y centos-release-scl; \
    # replace again after install centos-release-scl and before install devtoolset-11-toolchain
    sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo; \
    sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo; \
    sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo; \
    yum install -y devtoolset-11-toolchain devtoolset-11-libasan-devel; scl_source enable devtoolset-11; \
    sed -i 's|enabled=1|enabled=0|g' /etc/yum.repos.d/CentOS-SCLo-scl*.repo; \
    yum install -y jq sudo vim wget gdb gnutls-devel bison ccache rsync \
    # yum install -y jq sudo vim wget curl libcurl-devel curl4-openssl-devel gdb gnutls-devel bison ccache \
    cmake3 ninja-build make openssh-clients leveldb-devel openssl-devel snappy-devel openssl \
    lcov bzip2-devel lz4-devel libasan.x86_64 ncurses-devel libuv-devel.x86_64 dh-autoreconf.noarch java-11-openjdk-devel \
    redis tcl readline-devel awscli patchelf; \
    # update git version v2.37.1
    yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm && \
    yum install git -y; \
    yum install -y ca-certificates glibc-devel pkg-config curl unzip; \
    # install gflags version v2.2.1
    # wget https://linux.cc.iitk.ac.in/mirror/centos/7/cloud/x86_64/openstack-train/Packages/g/gflags-devel-2.2.1-1.el7.x86_64.rpm && \
    # wget https://linux.cc.iitk.ac.in/mirror/centos/7/cloud/x86_64/openstack-train/Packages/g/gflags-2.2.1-1.el7.x86_64.rpm && \
    # rpm -ivh gflags-* && \
    yum clean all; \
    ln -s /usr/bin/cmake3 /usr/bin/cmake;


# install aws cli
RUN set -ex; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip"; \
    unzip awscliv2.zip && rm awscliv2.zip; \
    ./aws/install && rm -r aws

# install rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# install cargo make(fail in centos7 for ring crate)
# RUN cargo install cargo-make

# Compile protobuf from source code.  Protobuf version need be compatibility
# with both brpc and grpc. It cannot be too high or too low.
RUN source scl_source enable devtoolset-11 && \
    mkdir -p $HOME/Downloads/protobuf && cd $HOME/Downloads/protobuf && \
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
    cd ../ && rm -rf protobuf