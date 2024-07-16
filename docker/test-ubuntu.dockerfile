ARG OS_ID
FROM ubuntu:${OS_ID}.04

RUN set -eux; \
    apt update; \
    apt install -y --no-install-recommends sudo curl ca-certificates openssh-server iproute2 redis-tools; \
    rm -rf /var/lib/apt/lists/*;

RUN useradd -rm -s /bin/bash -g sudo eloquser && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER eloquser
WORKDIR /home/eloquser

COPY ssh /home/eloquser/.ssh
RUN sudo chown -R eloquser /home/eloquser/.ssh && chmod 400 /home/eloquser/.ssh/*

EXPOSE 22
CMD ["sudo", "/usr/sbin/sshd", "-D"]