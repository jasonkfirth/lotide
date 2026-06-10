# Lotide Installation

Lotide is the backend server. It owns the PostgreSQL schema, the API,
ActivityPub endpoints, media handling, and background task worker. It does not
serve the normal browser UI by itself; install Hitide for that.

This guide documents the direct Debian source-tree install used by the live
Lotide instance. There are also helper scripts under `build_scripts/` that install
a more conventional `/usr/local/bin` layout, but the direct `/var/lotide`
layout is the one described here because it matches the wrapper script and
systemd unit that are known to start the current service successfully.

Do not copy live passwords from another instance. The examples below use
placeholders.

## What You Need

Gather these values before starting:

- `LOTIDE_DOMAIN`: public host name, for example `lotide.example.com`.
- `LOTIDE_DB_PASSWORD`: password for the PostgreSQL `lotide` role.
- `SMTP_USERNAME`: optional SMTP username.
- `SMTP_PASSWORD`: optional SMTP password.
- `SMTP_SERVER`: optional SMTP server host.
- `SMTP_FROM`: optional From address for outgoing mail.

Lotide listens on `127.0.0.1:3333` by default. Hitide normally listens on
`127.0.0.1:4333`. Nginx or another reverse proxy should expose both under the
public HTTPS host. If the reverse proxy runs on another host, set
`BIND_ADDRESS` to a reachable private address such as `0.0.0.0` or the server's
LAN address, then firewall the ports so only the proxy can connect.

## Debian Packages

Install the build and runtime packages:

```sh
sudo apt update
sudo apt install -y \
  build-essential \
  ca-certificates \
  curl \
  git \
  libssl-dev \
  nginx \
  openssl \
  pkg-config \
  postgresql \
  postgresql-client
```

Use a current Rust toolchain. The project uses Rust 2024, so `rustup` stable is
preferred over old distro Rust packages:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
rustup toolchain install stable
rustup component add clippy rustfmt
```

## System User

Create one service user for both Lotide and Hitide:

```sh
sudo adduser --system --group --home /var/lotide --shell /usr/sbin/nologin lotide
```

The live deployment runs both services as this same `lotide` user. That keeps
media and release files easy to reason about.

## PostgreSQL

Create the database role and database:

```sh
sudo -u postgres psql
```

```sql
CREATE USER lotide WITH PASSWORD 'LOTIDE_DB_PASSWORD';
CREATE DATABASE lotide OWNER lotide;
\q
```

For a normal local PostgreSQL install, use a `localhost` connection string and
do not set `DATABASE_CERTIFICATE_PATH`. Lotide builds a TLS-capable Postgres
connector, but with local PostgreSQL and no certificate path it does not need a
custom Postgres TLS certificate. OpenSSL is still needed for the application and
its HTTPS federation dependencies.

## Source Checkout

Clone the backend into `/var/lotide`:

```sh
cd /var
sudo git clone https://git.sr.ht/~vpzom/lotide/ lotide
sudo install -d -o lotide -g lotide -m 0750 /var/lotide/media
sudo chown -R lotide:lotide /var/lotide
```

If you are deploying this maintained tree instead of the public upstream,
copy or clone that tree into `/var/lotide` and keep the same ownership.

## Build

Build the release binary:

```sh
cd /var/lotide
cargo build --release --bin lotide
sudo chown -R lotide:lotide /var/lotide
```

The binary should exist at:

```sh
/var/lotide/target/release/lotide
```

## Wrapper Script

Create `/var/lotide/target/release/lotide.sh`:

```sh
#!/bin/bash

export DATABASE_URL="postgresql://lotide:LOTIDE_DB_PASSWORD@localhost:5432/lotide"
export LOTIDE_DB_POOL_MAX_SIZE="10"

export HOST_URL_ACTIVITYPUB="https://LOTIDE_DOMAIN/apub"
export HOST_URL_API="https://LOTIDE_DOMAIN/api"
export BIND_ADDRESS="127.0.0.1"

export APUB_PROXY_REWRITES=true
export ALLOW_FORWARDED=true

export BACKEND_HOST="http://127.0.0.1:3333"
export MEDIA_LOCATION="/var/lotide/media/"

# Optional mail configuration.
# export SMTP_URL="smtps://SMTP_USERNAME:SMTP_PASSWORD@SMTP_SERVER"
# export SMTP_FROM="SMTP_FROM"

cd /var/lotide/target/release

./lotide migrate
exec ./lotide
```

Then set ownership and permissions:

```sh
sudo chown lotide:lotide /var/lotide/target/release/lotide.sh
sudo chmod 0750 /var/lotide/target/release/lotide.sh
```

The important live behavior is the `./lotide migrate` line before `exec
./lotide`. Lotide checks for unapplied migrations during normal startup and will
refuse to run if the database is behind.

`BACKEND_HOST` is included because the live wrapper keeps a shared environment
shape with Hitide. Lotide does not currently read that setting.

For a brand new database, run migration setup once before starting the service:

```sh
sudo -u lotide env \
  DATABASE_URL="postgresql://lotide:LOTIDE_DB_PASSWORD@localhost:5432/lotide" \
  HOST_URL_ACTIVITYPUB="https://LOTIDE_DOMAIN/apub" \
  HOST_URL_API="https://LOTIDE_DOMAIN/api" \
  /var/lotide/target/release/lotide migrate setup
```

After that, the wrapper script can run normal migrations on every service start.

If your HTTPS reverse proxy is not on the Lotide host, change the wrapper to
bind the backend to an address the proxy can reach:

```sh
export BIND_ADDRESS="0.0.0.0"
```

Use a specific private interface address instead of `0.0.0.0` when that is
cleaner for your network. Keep `HOST_URL_ACTIVITYPUB` and `HOST_URL_API` set to
the public HTTPS URLs; `BIND_ADDRESS` only controls the local socket.

## Systemd

Create `/etc/systemd/system/lotide.service`:

```ini
[Unit]
Description=Lotide
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=lotide
Group=lotide
ExecStart=/var/lotide/target/release/lotide.sh
Restart=on-failure
RestartSec=10
KillMode=control-group
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Load the unit and start Lotide:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now lotide.service
sudo systemctl status lotide.service
```

Logs:

```sh
sudo journalctl -u lotide -f
```

## Reverse Proxy

This Nginx shape routes API and ActivityPub traffic to Lotide and normal browser
traffic to Hitide. Replace `LOTIDE_DOMAIN` with the real host name and configure
TLS with your preferred ACME client.

```nginx
server {
    listen 80;
    server_name LOTIDE_DOMAIN;

    client_max_body_size 1G;
    proxy_set_header X-Forwarded-For $remote_addr;

    location /api {
        proxy_pass http://127.0.0.1:3333;
    }

    location /apub {
        proxy_pass http://127.0.0.1:3333;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Path $request_uri;
    }

    location /.well-known {
        proxy_pass http://127.0.0.1:3333;
    }

    location / {
        set $apub 0;

        if ($http_accept ~* "(application/activity\+json)|(application/ld\+json; profile=\"https://www.w3.org/ns/activitystreams\")") {
            set $apub 1;
        }

        if ($http_content_type = application/activity+json) {
            set $apub 1;
        }

        if ($http_content_type = "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"") {
            set $apub 1;
        }

        if ($apub = 1) {
            rewrite ^(.*)$ /apub$1;
        }

        proxy_pass http://127.0.0.1:4333;
    }
}
```

Restart Nginx after editing:

```sh
sudo nginx -t
sudo systemctl restart nginx
```

## Configuration Reference

Required Lotide settings:

- `DATABASE_URL`: PostgreSQL URL.
- `HOST_URL_ACTIVITYPUB`: public ActivityPub base URL, usually
  `https://LOTIDE_DOMAIN/apub`.
- `HOST_URL_API`: public API base URL, usually `https://LOTIDE_DOMAIN/api`.

Common production settings:

- `LOTIDE_DB_POOL_MAX_SIZE`: maximum backend database pool size. The live
  single-user deployment uses `10`.
- `BIND_ADDRESS`: backend listen address. Defaults to `127.0.0.1`. Use
  `0.0.0.0`, `::`, or a private interface address when a reverse proxy on
  another host needs direct access.
- `PORT`: backend listen port. Defaults to `3333`.
- `APUB_PROXY_REWRITES`: set to `true` when the reverse proxy rewrites
  ActivityPub requests into `/apub`.
- `ALLOW_FORWARDED`: set to `true` when the reverse proxy sends forwarded
  client addresses.
- `MEDIA_LOCATION`: local media path. The live direct install uses
  `/var/lotide/media/`.
- `SMTP_URL`: optional SMTP URL, for example
  `smtps://user:password@smtp.example.com`.
- `SMTP_FROM`: required when `SMTP_URL` is set.
- `DATABASE_CERTIFICATE_PATH`: optional PEM certificate for PostgreSQL TLS. Do
  not set it for the normal local `localhost` PostgreSQL deployment.
- `RUST_LOG`: optional log filter, for example `lotide=info`.

Lotide also accepts variables with a `LOTIDE_` prefix. For example,
`DATABASE_URL` and `LOTIDE_DATABASE_URL` both map to the same field.

## Updating

For a direct source-tree install:

```sh
sudo systemctl stop hitide.service lotide.service
cd /var/lotide
git pull
cargo build --release --bin lotide
sudo chown -R lotide:lotide /var/lotide
sudo systemctl start lotide.service hitide.service
```

Start Lotide before Hitide. The Lotide wrapper runs migrations before the
backend process starts.

/* end of INSTALL.md */
