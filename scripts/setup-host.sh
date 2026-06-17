#!/usr/bin/env bash

set -euo pipefail

ELOQ_USER="${ELOQ_USER:-eloq}"
ELOQ_GROUP="${ELOQ_GROUP:-$ELOQ_USER}"
SSH_PUBKEY="${SSH_PUBKEY:-}"
HOSTNAME_VALUE="${HOSTNAME_VALUE:-}"

log() {
  printf '[setup-host] %s\n' "$*"
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Please run this script as root or through sudo." >&2
    exit 1
  fi
}

detect_pkg_manager() {
  if command -v apt-get >/dev/null 2>&1; then
    echo apt
    return
  fi
  if command -v dnf >/dev/null 2>&1; then
    echo dnf
    return
  fi
  if command -v yum >/dev/null 2>&1; then
    echo yum
    return
  fi
  echo "Unsupported package manager. Install dependencies manually." >&2
  exit 1
}

install_packages() {
  local pkg_manager
  pkg_manager="$(detect_pkg_manager)"

  case "${pkg_manager}" in
    apt)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y openssh-server sudo curl wget xz-utils tar rsync
      ;;
    dnf)
      dnf install -y openssh-server sudo curl wget xz tar rsync
      ;;
    yum)
      yum install -y openssh-server sudo curl wget xz tar rsync
      ;;
  esac
}

ensure_user() {
  if ! id -u "${ELOQ_USER}" >/dev/null 2>&1; then
    log "Creating user ${ELOQ_USER}"
    useradd -m -s /bin/bash "${ELOQ_USER}"
  fi

  if ! getent group "${ELOQ_GROUP}" >/dev/null 2>&1; then
    log "Creating group ${ELOQ_GROUP}"
    groupadd "${ELOQ_GROUP}"
  fi

  usermod -aG "${ELOQ_GROUP}" "${ELOQ_USER}" || true
}

configure_sudo() {
  cat >/etc/sudoers.d/90-eloqctl <<"EOF"
eloq ALL=(ALL) NOPASSWD: ALL
EOF
  chmod 0440 /etc/sudoers.d/90-eloqctl
}

configure_sshd() {
  local sshd_config="/etc/ssh/sshd_config"
  touch "${sshd_config}"

  sed -i '/^PasswordAuthentication /d' "${sshd_config}"
  sed -i '/^PubkeyAuthentication /d' "${sshd_config}"
  sed -i '/^AuthorizedKeysFile /d' "${sshd_config}"

  cat >>"${sshd_config}" <<"EOF"
PasswordAuthentication yes
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
EOF

  if systemctl list-unit-files | grep -q '^ssh\.service'; then
    systemctl enable --now ssh
    systemctl restart ssh
  elif systemctl list-unit-files | grep -q '^sshd\.service'; then
    systemctl enable --now sshd
    systemctl restart sshd
  fi
}

configure_limits() {
  cat >/etc/security/limits.d/90-eloq.conf <<"EOF"
* soft nofile 524288
* hard nofile 524288
* soft core unlimited
* hard core unlimited
EOF
}

configure_core_dump() {
  cat >/etc/sysctl.d/90-eloq-core.conf <<"EOF"
kernel.core_pattern=/var/crash/core-%e-%s-%u-%g-%p-%t
EOF
  sysctl --system >/dev/null

  mkdir -p /var/crash
  chown -R "${ELOQ_USER}:${ELOQ_GROUP}" /var/crash
}

configure_user_shell() {
  local user_home
  user_home="$(getent passwd "${ELOQ_USER}" | cut -d: -f6)"

  install -d -m 700 -o "${ELOQ_USER}" -g "${ELOQ_GROUP}" "${user_home}/.ssh"

  if [[ -n "${SSH_PUBKEY}" ]]; then
    touch "${user_home}/.ssh/authorized_keys"
    grep -qxF "${SSH_PUBKEY}" "${user_home}/.ssh/authorized_keys" || \
      printf '%s\n' "${SSH_PUBKEY}" >>"${user_home}/.ssh/authorized_keys"
    chown "${ELOQ_USER}:${ELOQ_GROUP}" "${user_home}/.ssh/authorized_keys"
    chmod 600 "${user_home}/.ssh/authorized_keys"
  fi

  if ! grep -qxF 'ulimit -c unlimited' "${user_home}/.bashrc"; then
    printf '\nulimit -c unlimited\n' >>"${user_home}/.bashrc"
  fi
  chown "${ELOQ_USER}:${ELOQ_GROUP}" "${user_home}/.bashrc"
}

configure_hostname() {
  if [[ -n "${HOSTNAME_VALUE}" ]]; then
    hostnamectl set-hostname "${HOSTNAME_VALUE}"
  fi
}

main() {
  require_root
  install_packages
  ensure_user
  configure_sudo
  configure_sshd
  configure_limits
  configure_core_dump
  configure_user_shell
  configure_hostname
  log "Host setup complete for user ${ELOQ_USER}"
}

main "$@"
