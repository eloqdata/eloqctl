FROM rockylinux:9

RUN set -eux; \
    dnf install -y epel-release; \
    dnf update -y; \
    dnf install -y sudo openssh-clients openssh-server iproute procps; \
    dnf clean all;

RUN useradd -rm -s /bin/bash -g root -G wheel eloquser && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    echo 'eloquser ALL=(root) NOPASSWD:ALL' >> /etc/sudoers

USER eloquser
WORKDIR /home/eloquser

COPY ssh /home/eloquser/.ssh
RUN sudo chown -R eloquser /home/eloquser/.ssh && chmod 400 /home/eloquser/.ssh/* && \
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N '' && \
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_dsa_key -N '' && \
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_ed25519_key -N '' && \
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_ecdsa_key -N '' && \
    if [ -f "/run/nologin" ]; then sudo rm /run/nologin; fi

EXPOSE 22
CMD ["sudo", "/usr/sbin/sshd", "-D"]