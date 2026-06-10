#!/usr/bin/env bash
#
# Project: Lotide Debian Install Support
# --------------------------------------
#
# File: debian-install-lotide.sh
#
# Purpose:
#
#     Install a previously built Lotide binary onto a Debian host.
#
# Responsibilities:
#
#     - install runtime packages
#     - create the lotide system user and state directories
#     - install a default environment file without overwriting local secrets
#     - install systemd units for the main server and optional worker
#
# This file intentionally does NOT contain:
#
#     - PostgreSQL role or database creation
#     - migrations, because they require real database credentials
#     - reverse proxy configuration

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PREFIX="${PREFIX:-/usr/local}"
BIN_DIR="${BIN_DIR:-${PREFIX}/bin}"
LOTIDE_USER="${LOTIDE_USER:-lotide}"
LOTIDE_GROUP="${LOTIDE_GROUP:-${LOTIDE_USER}}"
LOTIDE_CONFIG_DIR="${LOTIDE_CONFIG_DIR:-/etc/lotide}"
LOTIDE_ENV_FILE="${LOTIDE_ENV_FILE:-${LOTIDE_CONFIG_DIR}/lotide.env}"
LOTIDE_STATE_DIR="${LOTIDE_STATE_DIR:-/var/lib/lotide}"
LOTIDE_MEDIA_DIR="${LOTIDE_MEDIA_DIR:-${LOTIDE_STATE_DIR}/media}"
LOTIDE_BINARY="${LOTIDE_BINARY:-${PROJECT_DIR}/target/release/lotide}"

APT_PACKAGES=(
    ca-certificates
    openssl
    postgresql-client
)

require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "Run this install script as root." >&2
        exit 1
    fi
}

install_runtime_packages() {
    if [ ! -r /etc/debian_version ]; then
        echo "This script is intended for Debian or Debian-derived systems." >&2
        exit 1
    fi

    DEBIAN_FRONTEND=noninteractive apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${APT_PACKAGES[@]}"
}

install_lotide_user() {
    if ! getent group "${LOTIDE_GROUP}" >/dev/null; then
        groupadd --system "${LOTIDE_GROUP}"
    fi

    if ! id "${LOTIDE_USER}" >/dev/null 2>&1; then
        useradd \
            --system \
            --gid "${LOTIDE_GROUP}" \
            --home-dir "${LOTIDE_STATE_DIR}" \
            --shell /usr/sbin/nologin \
            "${LOTIDE_USER}"
    fi

    install -d -o "${LOTIDE_USER}" -g "${LOTIDE_GROUP}" -m 0750 "${LOTIDE_STATE_DIR}"
    install -d -o "${LOTIDE_USER}" -g "${LOTIDE_GROUP}" -m 0750 "${LOTIDE_MEDIA_DIR}"
    install -d -o root -g root -m 0755 "${LOTIDE_CONFIG_DIR}"
}

install_lotide_binary() {
    if [ ! -x "${LOTIDE_BINARY}" ]; then
        echo "Missing built binary: ${LOTIDE_BINARY}" >&2
        echo "Run build_scripts/debian-build-lotide.sh first." >&2
        exit 1
    fi

    install -d -o root -g root -m 0755 "${BIN_DIR}"
    install -o root -g root -m 0755 "${LOTIDE_BINARY}" "${BIN_DIR}/lotide"
}

install_lotide_environment() {
    if [ -e "${LOTIDE_ENV_FILE}" ]; then
        echo "Keeping existing ${LOTIDE_ENV_FILE}"
        return
    fi

    cat >"${LOTIDE_ENV_FILE}" <<EOF
# Lotide environment file.
# Edit these values before starting the service.

DATABASE_URL=postgresql://lotide:change-me@localhost:5432/lotide
HOST_URL_ACTIVITYPUB=https://example.com/apub
HOST_URL_API=https://example.com/api
BIND_ADDRESS=127.0.0.1
PORT=3333

APUB_PROXY_REWRITES=true
ALLOW_FORWARDED=true
MEDIA_STORAGE=local
MEDIA_LOCATION=${LOTIDE_MEDIA_DIR}

# SMTP_URL=smtps://username:password@smtp.example.com
# SMTP_FROM=webmaster@example.com

# Set this to true only when running lotide-worker.service too.
SEPARATE_WORKER=false
RUST_LOG=lotide=info
EOF

    chown root:"${LOTIDE_GROUP}" "${LOTIDE_ENV_FILE}"
    chmod 0640 "${LOTIDE_ENV_FILE}"
}

install_systemd_units() {
    cat >/etc/systemd/system/lotide.service <<EOF
[Unit]
Description=Lotide ActivityPub server
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=${LOTIDE_USER}
Group=${LOTIDE_GROUP}
WorkingDirectory=${LOTIDE_STATE_DIR}
EnvironmentFile=${LOTIDE_ENV_FILE}
ExecStart=${BIN_DIR}/lotide
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=full
ReadWritePaths=${LOTIDE_STATE_DIR}

[Install]
WantedBy=multi-user.target
EOF

    cat >/etc/systemd/system/lotide-worker.service <<EOF
[Unit]
Description=Lotide background worker
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=${LOTIDE_USER}
Group=${LOTIDE_GROUP}
WorkingDirectory=${LOTIDE_STATE_DIR}
EnvironmentFile=${LOTIDE_ENV_FILE}
ExecStart=${BIN_DIR}/lotide worker
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=full
ReadWritePaths=${LOTIDE_STATE_DIR}

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
}

require_root
install_runtime_packages
install_lotide_user
install_lotide_binary
install_lotide_environment
install_systemd_units

cat <<EOF
Lotide has been installed.

Next steps:
  1. Edit ${LOTIDE_ENV_FILE}.
  2. Create the PostgreSQL role and database referenced by DATABASE_URL.
  3. Run migrations:
       runuser -u ${LOTIDE_USER} --shell /bin/bash --command 'set -a; . ${LOTIDE_ENV_FILE}; set +a; ${BIN_DIR}/lotide migrate setup'
       runuser -u ${LOTIDE_USER} --shell /bin/bash --command 'set -a; . ${LOTIDE_ENV_FILE}; set +a; ${BIN_DIR}/lotide migrate'
  4. Start the service:
       systemctl enable --now lotide.service

Enable lotide-worker.service only if SEPARATE_WORKER=true.
EOF

# end of debian-install-lotide.sh
