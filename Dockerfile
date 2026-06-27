FROM docker.redpanda.com/redpandadata/redpanda:v24.1.6 AS redpanda

FROM node:22-bookworm-slim AS build

WORKDIR /app

COPY package.json ./
COPY pnpm-lock.yaml ./
COPY tsconfig.json ./
COPY src ./src

RUN corepack enable \
 && pnpm install --frozen-lockfile \
 && pnpm run build \
 && pnpm prune --prod

FROM node:22-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production

COPY --from=redpanda /usr/bin/rpk /usr/local/bin/rpk

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

USER node

ENTRYPOINT ["node", "/app/dist/cli.js"]
CMD ["reconcile"]