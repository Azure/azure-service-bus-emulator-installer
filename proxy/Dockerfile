FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm ci --ignore-scripts
COPY src/ src/
RUN npx tsc

FROM node:20-alpine
RUN addgroup -S proxy && adduser -S proxy -G proxy
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY package.json ./
USER proxy
EXPOSE 5672
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "require('net').connect(5672).on('connect',()=>process.exit(0)).on('error',()=>process.exit(1))"
CMD ["node", "dist/index.js"]
