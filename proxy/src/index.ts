import rhea = require('rhea');
import pino from 'pino';
import { loadConfig } from './config';
import { SessionPool } from './session-pool';
import { SessionRelay } from './session-relay';

const config = loadConfig();
const logger = pino({ level: config.logLevel });

logger.info({
  proxyPort: config.proxyPort,
  backendHost: config.backendHost,
  backendPort: config.backendPort,
  maxBackendConnections: config.maxBackendConnections,
  maxClientConnections: config.maxClientConnections,
}, 'starting AMQP session-multiplexing proxy');

const serverContainer = rhea.create_container({ id: 'amqp-proxy-server' });

(serverContainer.sasl_server_mechanisms as any)['MSSBCBS'] = function () {
  return {
    start: function (this: any) { this.outcome = true; this.username = 'proxy-bypass'; },
    step: function (this: any) { this.outcome = true; },
    outcome: true,
  };
};
(serverContainer.sasl_server_mechanisms as any)['ANONYMOUS'] = function () {
  return { start: function (this: any) { this.outcome = true; }, outcome: true };
};
(serverContainer.sasl_server_mechanisms as any)['PLAIN'] = function () {
  return { start: function (this: any) { this.outcome = true; }, outcome: true };
};
(serverContainer.sasl_server_mechanisms as any)['EXTERNAL'] = function () {
  return { start: function (this: any) { this.outcome = true; }, outcome: true };
};

const clientContainer = rhea.create_container({ id: 'amqp-proxy-client' });

const pool = new SessionPool(clientContainer, config, logger);
const relay = new SessionRelay(pool, logger);

let activeClientConnections = 0;
let shuttingDown = false;
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
  if (stats.totalSessions > 0 || activeClientConnections > 0) {
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

  if (activeClientConnections > config.maxClientConnections) {
    logger.warn({ connId, activeClientConnections, max: config.maxClientConnections }, 'client connection limit reached');
    context.connection.close({ condition: 'amqp:resource-limit-exceeded', description: 'Max client connections reached' });
    activeClientConnections--;
    return;
  }

  logger.info({ connId, activeClientConnections }, 'client connected');
});

serverContainer.on('connection_close', (context: rhea.EventContext) => {
  const connId = getClientConnectionId(context.connection);
  activeClientConnections = Math.max(0, activeClientConnections - 1);
  logger.info({ connId, activeClientConnections }, 'client disconnected');
  relay.cleanupConnection(connId);
});

serverContainer.on('disconnected', (context: rhea.EventContext) => {
  const connId = getClientConnectionId(context.connection);
  activeClientConnections = Math.max(0, activeClientConnections - 1);
  logger.info({ connId }, 'client disconnected (unclean)');
  relay.cleanupConnection(connId);
});

clientContainer.on('disconnected', (context: rhea.EventContext) => {
  const affectedClients = pool.handleDisconnect(context.connection);
  if (affectedClients.length > 0) {
    logger.warn({ affectedClients: affectedClients.length }, 'backend connection lost');
  }
});

serverContainer.on('session_open', (context: rhea.EventContext) => {
  const connId = getClientConnectionId(context.connection);
  interceptSessionAttach(context.session);
  relay.handleSessionOpen(connId, context.session);
});

function interceptSessionAttach(session: any): void {
  const originalOnAttach = session.on_attach.bind(session);
  session.on_attach = (frame: any) => {
    const perf = frame.performative;
    originalOnAttach(frame);

    const link = session.links[perf.name];
    if (link?.local?.attach) {
      if (perf.source && !link.local.attach.source) link.local.attach.source = perf.source;
      if (perf.target && !link.local.attach.target) link.local.attach.target = perf.target;
      if (!link.local.attach.max_message_size) {
        link.local.attach.max_message_size = 262144;
      }
    }
  };
}

serverContainer.on('sender_open', (context: rhea.EventContext) => {
  try {
    relay.handleSenderOpen(context);
  } catch (err) {
    logger.error({ err }, 'error handling sender_open');
    context.sender?.close({ condition: 'amqp:internal-error' });
  }
});

serverContainer.on('receiver_open', (context: rhea.EventContext) => {
  try {
    relay.handleReceiverOpen(context);
  } catch (err) {
    logger.error({ err }, 'error handling receiver_open');
    context.receiver?.close({ condition: 'amqp:internal-error' });
  }
});

serverContainer.on('message', (context: rhea.EventContext) => {
  try {
    relay.handleMessage(context);
  } catch (err) {
    logger.error({ err }, 'error handling message');
    context.delivery?.reject({ condition: 'amqp:internal-error' });
  }
});

serverContainer.on('settled', (context: rhea.EventContext) => {
  try { relay.handleSettled(context); } catch (err) { logger.error({ err }, 'error handling settled'); }
});

serverContainer.on('accepted', (context: rhea.EventContext) => {
  try { relay.handleAccepted(context); } catch (err) { logger.error({ err }, 'error handling accepted'); }
});

serverContainer.on('rejected', (context: rhea.EventContext) => {
  try { relay.handleRejected(context); } catch (err) { logger.error({ err }, 'error handling rejected'); }
});

serverContainer.on('released', (context: rhea.EventContext) => {
  try { relay.handleReleased(context); } catch (err) { logger.error({ err }, 'error handling released'); }
});

serverContainer.on('modified', (context: rhea.EventContext) => {
  try { relay.handleModified(context); } catch (err) { logger.error({ err }, 'error handling modified'); }
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
    idle_time_out: 120000,
    properties: {
      product: 'Microsoft.Azure.ServiceBus.Emulator',
      version: '1.0',
    },
    sender_options: { autosettle: false },
    receiver_options: { autoaccept: false, credit_window: 100 },
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
  logger.info({ signal, activeClientConnections, ...stats }, 'shutting down');

  const server = (global as any).__server;
  if (server) server.close();

  setTimeout(() => {
    relay.shutdown();
    pool.closeAll();
    logger.info('shutdown complete');
    process.exit(0);
  }, 5000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('uncaughtException', (err: any) => {
  if (err?.condition === 'amqp:invalid-field' || err?.condition === 'amqp:internal-error') {
    logger.warn({ err: { type: err.name, condition: err.condition, description: err.description } }, 'non-fatal AMQP error');
    return;
  }
  logger.fatal({ err }, 'uncaught exception');
  shutdown('uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  logger.fatal({ reason }, 'unhandled rejection');
});
