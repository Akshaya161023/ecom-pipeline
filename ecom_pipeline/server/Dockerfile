# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Build the React dashboard (Vite)
# Build context must be the ecom_pipeline/ folder so we can reach both
# dashboard/ and server/ in a single Docker build.
# ─────────────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS dashboard-build

WORKDIR /build/dashboard

# Install dependencies first (cache layer)
COPY dashboard/package*.json ./
RUN npm ci

# Copy source and build
COPY dashboard/ ./
RUN npm run build

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Production Node.js server
# ─────────────────────────────────────────────────────────────────────────────
FROM node:20-alpine

WORKDIR /app

# Install production deps only
COPY server/package*.json ./
RUN npm ci --only=production

# Copy server source
COPY server/ ./

# Copy the built React app into server/public so Express can serve it
COPY --from=dashboard-build /build/dashboard/dist ./public

EXPOSE 3001

CMD ["node", "index.js"]
