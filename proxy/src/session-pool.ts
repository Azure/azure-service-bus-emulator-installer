import rhea = require('rhea');
import { Config } from './config';
import pino from 'pino';

interface BackendConnection {
  id: string;
  conn: rhea.Connection;
  sessionCount: number;
}

interface ClientSession {
  session: rhea.Session;
  backendConnId: string;
  ready: boolean;
}

export class SessionPool {
  private connections: BackendConnection[] = [];
  private clientSessions = new Map<string, ClientSession>();
  private nextConnId = 0;
  private readonly container: rhea.Container;
  private readonly logger: pino.Logger;
  private readonly config: Config;

  constructor(container: rhea.Container, config: Config, logger: pino.Logger) {
    this.container = container;
    this.config = config;
    this.logger = logger.child({ component: 'session-pool' });
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

  async acquireSession(clientConnectionId: string): Promise<rhea.Session> {
    const existing = this.clientSessions.get(clientConnectionId);
    if (existing?.ready) return existing.session;

    let backend = this.getLeastLoadedConnection();
    if (!backend) {
      await this.createConnection();
      backend = this.getLeastLoadedConnection();
      if (!backend) {
        throw new Error('No backend connections available');
      }
    }

    return new Promise((resolve, reject) => {
      const session = backend.conn.create_session();

      const timeout = setTimeout(() => {
        reject(new Error('Backend session open timed out'));
      }, 10_000);

      session.on('session_open', () => {
        clearTimeout(timeout);
        backend.sessionCount++;
        const entry: ClientSession = { session, backendConnId: backend.id, ready: true };
        this.clientSessions.set(clientConnectionId, entry);
        this.logger.info({ clientConnectionId, backendConnId: backend.id, sessionCount: backend.sessionCount }, 'backend session created');
        resolve(session);
      });

      session.on('session_error', (ctx) => {
        clearTimeout(timeout);
        this.logger.error({ clientConnectionId, error: (ctx as any).session?.error }, 'backend session error');
        reject(new Error('Backend session error'));
      });

      session.on('session_close', () => {
        const entry = this.clientSessions.get(clientConnectionId);
        if (entry) {
          this.clientSessions.delete(clientConnectionId);
          const conn = this.connections.find(c => c.id === entry.backendConnId);
          if (conn) conn.sessionCount = Math.max(0, conn.sessionCount - 1);
        }
      });

      session.begin();
    });
  }

  getSession(clientConnectionId: string): rhea.Session | null {
    const entry = this.clientSessions.get(clientConnectionId);
    return entry?.ready ? entry.session : null;
  }

  releaseSession(clientConnectionId: string): void {
    const entry = this.clientSessions.get(clientConnectionId);
    if (!entry) return;

    try {
      if (entry.session.is_open()) {
        entry.session.close();
      }
    } catch { /* ignore close errors */ }

    const conn = this.connections.find(c => c.id === entry.backendConnId);
    if (conn) conn.sessionCount = Math.max(0, conn.sessionCount - 1);

    this.clientSessions.delete(clientConnectionId);
    this.logger.info({ clientConnectionId, backendConnId: entry.backendConnId }, 'backend session released');
  }

  handleDisconnect(conn: rhea.Connection): string[] {
    const idx = this.connections.findIndex(e => e.conn === conn);
    if (idx === -1) return [];

    const deadId = this.connections[idx].id;
    this.connections.splice(idx, 1);
    this.logger.warn({ connId: deadId }, 'backend connection died');

    const affected: string[] = [];
    for (const [clientId, entry] of this.clientSessions) {
      if (entry.backendConnId === deadId) {
        affected.push(clientId);
        this.clientSessions.delete(clientId);
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
    this.clientSessions.clear();
  }

  stats() {
    const open = this.connections.filter(e => e.conn.is_open()).length;
    const totalSessions = this.clientSessions.size;
    return { total: this.connections.length, healthy: open, dead: this.connections.length - open, totalSessions };
  }

  private getLeastLoadedConnection(): BackendConnection | null {
    let best: BackendConnection | null = null;
    for (const entry of this.connections) {
      if (entry.conn.is_open() && (!best || entry.sessionCount < best.sessionCount)) {
        best = entry;
      }
    }
    return best;
  }

  private async createInitialConnections(): Promise<void> {
    // Don't create connections eagerly - the emulator kills idle connections
    // after 60s. Connections are created on-demand when clients connect.
    this.logger.info('backend pool ready (connections created on demand)');
  }

  private createConnection(): Promise<void> {
    const connId = `backend-${this.nextConnId++}`;

    return new Promise((resolve, reject) => {
      const conn = this.container.connect({
        host: this.config.backendHost,
        port: this.config.backendPort,
        hostname: 'localhost',
        reconnect: false,
        max_frame_size: this.config.maxFrameSize,
        idle_time_out: 120_000,
        username: 'RootManageSharedAccessKey',
        password: 'SAS_KEY_VALUE',
      });

      const timeout = setTimeout(() => {
        reject(new Error(`Connection ${connId} timed out`));
      }, 10_000);

      conn.on('connection_open', () => {
        clearTimeout(timeout);
        this.connections.push({ id: connId, conn, sessionCount: 0 });
        this.logger.info({ connId }, 'backend connection established');
        resolve();
      });

      conn.on('connection_error', (ctx) => {
        this.logger.error({ connId, error: (ctx as any).error || conn.error }, 'backend connection error');
      });

      conn.on('disconnected', () => {
        clearTimeout(timeout);
        this.logger.warn({ connId }, 'backend disconnected event');
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
}
