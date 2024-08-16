ARG UBT_ID=24.04
FROM ubuntu:$UBT_ID

RUN set -eux; \
    apt update; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt install -y --no-install-recommends sudo curl ca-certificates openssh-server iproute2 redis-tools; \
    rm -rf /var/lib/apt/lists/*;

RUN useradd -rm -s /bin/bash -g sudo eloquser && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER eloquser
WORKDIR /home/eloquser

COPY ssh /home/eloquser/.ssh
RUN sudo chown -R eloquser /home/eloquser/.ssh && chmod 400 /home/eloquser/.ssh/* && \
    sudo mkdir /run/sshd

EXPOSE 22
CMD ["sudo", "/usr/sbin/sshd", "-D"]