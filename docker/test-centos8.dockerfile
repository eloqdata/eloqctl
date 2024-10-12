FROM centos:8

RUN set -eux; \
    # https://stackoverflow.com/questions/70963985/error-failed-to-download-metadata-for-repo-appstream-cannot-prepare-internal
    sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* ; \
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* ; \
    dnf install -y epel-release; \
    dnf update -y; \
    dnf install -y sudo openssh-clients openssh-server iproute python3 git; \
    dnf clean all;

RUN useradd -rm -s /bin/bash -g root -G wheel eloquser && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    echo 'eloquser ALL=(root) NOPASSWD:ALL' >> /etc/sudoers

USER eloquser
WORKDIR /home/eloquser

COPY ssh /home/eloquser/.ssh
USER root
RUN chown -R eloquser /home/eloquser/.ssh && chmod 400 /home/eloquser/.ssh/* && \
    ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N '' && \
    ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key -N '' && \
    ssh-keygen -t ed25519 -f /etc/ssh/ssh_host_ed25519_key -N '' && \
    ssh-keygen -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key -N '' && \
    if [ -f "/run/nologin" ]; then rm /run/nologin; fi
USER eloquser

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
