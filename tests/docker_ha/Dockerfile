FROM ubuntu:24.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        gdb \
        gdbserver \
        iproute2 \
        less \
        lsof \
        net-tools \
        openssh-server \
        procps \
        redis-tools \
        rsync \
        file \
        strace \
        sudo \
        vim-tiny \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash eloq \
    && echo "eloq ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/eloq \
    && chmod 440 /etc/sudoers.d/eloq \
    && mkdir -p /var/run/sshd /home/eloq/.ssh \
    && chmod 700 /home/eloq/.ssh \
    && chown -R eloq:eloq /home/eloq/.ssh

COPY authorized_keys /home/eloq/.ssh/authorized_keys
RUN chmod 600 /home/eloq/.ssh/authorized_keys \
    && chown eloq:eloq /home/eloq/.ssh/authorized_keys \
    && ssh-keygen -A \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config \
    && sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config \
    && printf '\nAuthorizedKeysFile .ssh/authorized_keys\n' >> /etc/ssh/sshd_config \
    && printf '* soft nofile 524288\n* hard nofile 524288\n* hard core unlimited\n* soft core unlimited\n' >> /etc/security/limits.conf \
    && ln -sf /etc/ssl/certs /usr/lib/ssl/certs \
    && mkdir -p /data \
    && chown -R eloq:eloq /data

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-e"]
