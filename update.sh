#!/bin/bash

export DATABASE_URL="postgresql://lotide:change-me@localhost:5432/lotide"

export HOST_URL_ACTIVITYPUB="https://example.com/apub"

export HOST_URL_API="https://example.com/api"

export FRONTEND_URL="https://example.com"

export APUB_PROXY_REWRITES=true

export ALLOW_FORWARDED=true

export BACKEND_HOST="http://127.0.0.1:3333"

export MEDIA_LOCATION="/var/lotide/media/"

# export SMTP_URL="smtps://username:password@smtp.example.com"

# export SMTP_FROM="webmaster@example.com"

#~/.cargo/bin/cargo run -- migrate setup
~/.cargo/bin/cargo run -- migrate
