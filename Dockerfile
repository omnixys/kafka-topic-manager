FROM node:22-bookworm-slim AS build
WORKDIR /app

COPY package.json ./
COPY tsconfig.json ./
COPY src ./src

RUN corepack enable \
 && pnpm install --frozen-lockfile=false \
 && pnpm run build \
 && pnpm prune --prod

FROM node:22-bookworm-slim

RUN apt-get update \
 && apt-get install -y curl ca-certificates \
 && curl -fsSL https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip -o /tmp/rpk.zip