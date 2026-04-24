import rhea = require('rhea');
import { Config } from './config';
import pino from 'pino';

export class ConnectionPool {
  private connections: Array<{ id: string; conn: rhea.Connection; linkCount: number }> = [];
  private clientLinks = new Map<string, Set<string>>();
  private nextId = 0;
  private readonly container: rhea.Container;
  private readonly logger: pino.Logger;
  private readonly config: Config;

  constructor(container: rhea.Container, config: Config, logger: pino.Logger) {
    this.container = container;
    this.config = config;
    this.logger = logger.child({ component: 'connection-pool' });
  }

  async initialize(): Promise<void> {
    const net = require('net');
    const start = Date.now();
    const timeout = 120_000;

    this.logger.info({ host: this.config.backendHost, port: this.config.backendPort }, 'waiting for backend emulator');

    while (Date.now() - start < timeout) {
      const reachable = await new Promise<boolean>((resolve) => {
        const sock = net.createConnection({ host: this.config.backendHost, port: this.config.backendPort, timeout: 3000 });
        sock.on('connect', () => { sock.destroy(); resolve(true); });
        sock.on('error', () => { sock.destroy(); resolve(false); });
        sock.on('timeout', () => { sock.destroy(); resolve(false); });
      });

      if (reachable) {
        this.logger.info('backend emulator is reachable, creating initial connection pool');
        await this.createInitialConnections();
        return;
      }

      await new Promise(r => setTimeout(r, 3000));
    }

    throw new Error('Backend emulator not available');
  }

  private clientAffinity = new Map<string, string>();

  acquire(clientConnectionId: string): { connId: string; conn: rhea.Connection } | null {
    const pinnedId = this.clientAffinity.get(clientConnectionId);
    if (pinnedId) {
      const pinned = this.connections.find(e => e.id === pinnedId && e.conn.is_open());
      if (pinned) {
        pinned.linkCount++;
        this.trackClientLink(clientConnectionId, pinned.id);
        this.logger.debug({ connId: pinned.id, linkCount: pinned.linkCount }, 'acquired pinned backend connection');
        return { connId: pinned.id, conn: pinned.conn };
      }
      this.clientAffinity.delete(clientConnectionId);
    }

    let best: typeof this.connections[0] | null = null;
    for (const entry of this.connections) {
      if (entry.conn.is_open() && (!best || entry.linkCount < best.linkCount)) {
        best = entry;
      }
    }

    if (!best) {
      this.logger.warn('no open backend connections available');
      return null;
    }

    this.clientAffinity.set(clientConnectionId, best.id);
    best.linkCount++;
    this.trackClientLink(clientConnectionId, best.id);
    this.logger.debug({ connId: best.id, linkCount: best.linkCount }, 'acquired and pinned backend connection');
    return { connId: best.id, conn: best.conn };
  }

  releaseClientLinks(clientConnectionId: string): void {
    const connIds = this.clientLinks.get(clientConnectionId);
    if (!connIds) return;

    for (const connId of connIds) {
      const entry = this.connections.find(e => e.id === connId);
      if (entry) entry.linkCount = Math.max(0, entry.linkCount - 1);
    }

    this.clientLinks.delete(clientConnectionId);
    this.clientAffinity.delete(clientConnectionId);
  }

  decrementLinkCount(connId: string): void {
    const entry = this.connections.find(e => e.id === connId);
    if (entry) entry.linkCount = Math.max(0, entry.linkCount - 1);
  }

  handleDisconnect(conn: rhea.Connection): string[] {
    const idx = this.connections.findIndex(e => e.conn === conn);
    if (idx === -1) return [];

    const deadId = this.connections[idx].id;
    this.connections.splice(idx, 1);
    this.logger.warn({ connId: deadId }, 'backend connection died');

    const affected: string[] = [];
    for (const [clientId, connIds] of this.clientLinks) {
      if (connIds.has(deadId)) {
        affected.push(clientId);
        connIds.delete(deadId);
      }
    }

    this.scheduleReconnect();
    return affected;
  }

  closeAll(): void {
    for (const entry of this.connections) {
      try { entry.conn.close(); } catch {}
    }
    this.connections = [];
    this.clientLinks.clear();
  }

  stats() {
    const open = this.connections.filter(e => e.conn.is_open()).length;
    const totalLinks = this.connections.reduce((sum, e) => sum + e.linkCount, 0);
    return { total: this.connections.length, healthy: open, dead: this.connections.length - open, totalLinks };
  }

  private async createInitialConnections(): Promise<void> {
    const count = Math.min(2, this.config.maxBackendConnections);

    for (let i = 0; i < count; i++) {
      await this.createConnection();
    }

    this.logger.info({ count: this.connections.length }, 'initial backend connections established');
  }

  private createConnection(): Promise<void> {
    const connId = `backend-${this.nextId++}`;

    return new Promise((resolve, reject) => {
      const conn = this.container.connect({
        host: this.config.backendHost,
        port: this.config.backendPort,
        hostname: 'localhost',
        reconnect: false,
        max_frame_size: this.config.maxFrameSize,
        idle_time_out: 120_000,
      });

      const timeout = setTimeout(() => {
        reject(new Error(`Connection ${connId} timed out`));
      }, 10_000);

      const onOpen = () => {
        clearTimeout(timeout);
        conn.removeListener('connection_open', onOpen);
        this.connections.push({ id: connId, conn, linkCount: 0 });
        this.logger.info({ connId }, 'backend connection established');
        resolve();
      };

      conn.on('connection_open', onOpen);
      conn.on('connection_error', (ctx) => {
        this.logger.error({ connId, error: (ctx as any).error || conn.error }, 'backend connection error');
      });

      conn.on('disconnected', (ctx) => {
        clearTimeout(timeout);
        const err = (ctx as any).error;
        this.logger.warn({ connId, error: err?.message, reconnecting: (ctx as any).reconnecting }, 'backend disconnected event');
        this.handleDisconnect(conn);
      });
    });
  }

  private reconnecting = false;

  private scheduleReconnect(): void {
    if (this.reconnecting) return;
    this.reconnecting = true;

    const attempt = (delay: number, retries: number) => {
      if (retries <= 0) {
        this.logger.error('reconnection failed after max retries');
        this.reconnecting = false;
        return;
      }

      setTimeout(async () => {
        try {
          await this.createConnection();
          this.reconnecting = false;
          this.logger.info('reconnected to backend');
        } catch {
          attempt(Math.min(delay * 2, 30_000), retries - 1);
        }
      }, delay);
    };

    attempt(1000, 10);
  }

  private trackClientLink(clientConnectionId: string, backendConnId: string): void {
    let links = this.clientLinks.get(clientConnectionId);
    if (!links) {
      links = new Set();
      this.clientLinks.set(clientConnectionId, links);
    }
    links.add(backendConnId);
  }
}
