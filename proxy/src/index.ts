import rhea = require('rhea');
import pino from 'pino';
import { loadConfig } from './config';
import { ConnectionPool } from './connection-pool';
import { CbsHandler } from './cbs-handler';
import { LinkRelay } from './link-relay';

const config = loadConfig();
const logger = pino({ level: config.logLevel });

logger.warn('CBS authentication is bypassed — this proxy is for local development only');
logger.info({
  proxyPort: config.proxyPort,
  backendHost: config.backendHost,
  backendPort: config.backendPort,
  maxBackendConnections: config.maxBackendConnections,
  maxClientConnections: config.maxClientConnections,
}, 'starting AMQP connection multiplexing proxy');

const serverContainer = rhea.create_container({ id: 'amqp-proxy-server' });

(serverContainer.sasl_server_mechanisms as any)['MSSBCBS'] = function () {
  return {
    start: function (this: any, _response: any, _hostname: any) {
      this.outcome = true;
      this.username = 'proxy-bypass';
    },
    step: function (this: any, _challenge: any) {
      this.outcome = true;
    },
    outcome: true,
  };
};

(serverContainer.sasl_server_mechanisms as any)['ANONYMOUS'] = function () {
  return {
    start: function (this: any) { this.outcome = true; },
    outcome: true,
  };
};
const clientContainer = rhea.create_container({ id: 'amqp-proxy-client' });

const pool = new ConnectionPool(clientContainer, config, logger);
const cbsHandler = new CbsHandler(logger);
const linkRelay = new LinkRelay(pool, cbsHandler, logger);

let activeClientConnections = 0;
let shuttingDown = false;

const clientConnectionIds = new Set<string>();
let connectionCounter = 0;

function getClientConnectionId(conn: rhea.Connection): string {
  const existing = (conn as any).__proxyId;
  if (existing) return existing;
  const id = `client-${++connectionCounter}`;
  (conn as any).__proxyId = id;
  return id;
}

setInterval(() => {
  if (shuttingDown) return;
  const stats = pool.stats();
  if (stats.totalLinks > 0 || activeClientConnections > 0) {
    logger.info({ activeClientConnections, ...stats }, 'proxy stats');
  }
}, 30_000);

serverContainer.on('connection_open', (context: rhea.EventContext) => {
  if (shuttingDown) {
    context.connection.close({ condition: 'amqp:connection:forced', description: 'Proxy shutting down' });
    return;
  }

  const connId = getClientConnectionId(context.connection);
  activeClientConnections++;
  clientConnectionIds.add(connId);

  if (activeClientConnections > config.maxClientConnections) {
    logger.warn({ connId, activeClientConnections, max: config.maxClientConnections }, 'client connection limit reached');
    context.connection.close({ condition: 'amqp:resource-limit-exceeded', description: 'Max client connections reached' });
    activeClientConnections--;
    clientConnectionIds.delete(connId);
    return;
  }

  logger.info({ connId, activeClientConnections }, 'client connected');
});

serverContainer.on('connection_close', (context: rhea.EventContext) => {
  const connId = getClientConnectionId(context.connection);
  if (clientConnectionIds.delete(connId)) {
    activeClientConnections = Math.max(0, activeClientConnections - 1);
    logger.info({ connId, activeClientConnections }, 'client disconnected');
    linkRelay.cleanupConnection(connId);
  }
});

serverContainer.on('disconnected', (context: rhea.EventContext) => {
  const connId = getClientConnectionId(context.connection);
  if (clientConnectionIds.delete(connId)) {
    activeClientConnections = Math.max(0, activeClientConnections - 1);
    logger.info({ connId }, 'client disconnected (unclean)');
    linkRelay.cleanupConnection(connId);
  }
});

clientContainer.on('disconnected', (context: rhea.EventContext) => {
  const affectedClients = pool.handleDisconnect(context.connection);
  if (affectedClients.length > 0) {
    logger.warn({ affectedClients: affectedClients.length }, 'backend connection lost');
  }
});

serverContainer.on('session_open', () => {});

serverContainer.on('sender_open', (context: rhea.EventContext) => {
  try {
    linkRelay.handleSenderOpen(context);
  } catch (err) {
    logger.error({ err }, 'error handling sender_open');
    context.sender?.close({ condition: 'amqp:internal-error' });
  }
});

serverContainer.on('receiver_open', (context: rhea.EventContext) => {
  try {
    linkRelay.handleReceiverOpen(context);
  } catch (err) {
    logger.error({ err }, 'error handling receiver_open');
    context.receiver?.close({ condition: 'amqp:internal-error' });
  }
});

serverContainer.on('message', (context: rhea.EventContext) => {
  try {
    linkRelay.handleMessage(context);
  } catch (err) {
    logger.error({ err }, 'error handling message');
    context.delivery?.reject({ condition: 'amqp:internal-error' });
  }
});

serverContainer.on('settled', (context: rhea.EventContext) => {
  try {
    linkRelay.handleSettled(context);
  } catch (err) {
    logger.error({ err }, 'error handling settled');
  }
});

serverContainer.on('accepted', (context: rhea.EventContext) => {
  try { linkRelay.handleAccepted(context); } catch (err) { logger.error({ err }, 'error handling accepted'); }
});

serverContainer.on('rejected', (context: rhea.EventContext) => {
  try { linkRelay.handleRejected(context); } catch (err) { logger.error({ err }, 'error handling rejected'); }
});

serverContainer.on('released', (context: rhea.EventContext) => {
  try { linkRelay.handleReleased(context); } catch (err) { logger.error({ err }, 'error handling released'); }
});

serverContainer.on('modified', (context: rhea.EventContext) => {
  try { linkRelay.handleModified(context); } catch (err) { logger.error({ err }, 'error handling modified'); }
});

async function start(): Promise<void> {
  try {
    await pool.initialize();
  } catch (err) {
    logger.fatal({ err }, 'failed to connect to backend emulator');
    process.exit(1);
  }

  const server = serverContainer.listen({
    port: config.proxyPort,
    max_frame_size: config.maxFrameSize,
    sender_options: { autosettle: false },
    receiver_options: { autoaccept: false },
  } as any);

  server.on('listening', () => {
    logger.info({ port: config.proxyPort }, 'AMQP proxy listening — ready for connections');
  });

  server.on('error', (err: Error) => {
    logger.fatal({ err }, 'server error');
    process.exit(1);
  });

  (global as any).__server = server;
}

start();

function shutdown(signal: string): void {
  if (shuttingDown) return;
  shuttingDown = true;

  const stats = pool.stats();
  logger.info({ signal, activeClientConnections, ...stats }, 'shutting down — stopping new connections');

  const server = (global as any).__server;
  if (server) server.close();

  const drainTimeout = 5000;
  logger.info({ drainTimeout }, 'draining in-flight messages');

  setTimeout(() => {
    linkRelay.shutdown();
    pool.closeAll();
    logger.info('shutdown complete');
    process.exit(0);
  }, drainTimeout);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  logger.fatal({ err }, 'uncaught exception');
  shutdown('uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  logger.fatal({ reason }, 'unhandled rejection');
});
