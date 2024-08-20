FROM centos:8

RUN set -ex; \
    # https://stackoverflow.com/questions/70963985/error-failed-to-download-metadata-for-repo-appstream-cannot-prepare-internal
    sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* ; \
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* ; \
    dnf update -y; \
    dnf install -y epel-release; \
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
