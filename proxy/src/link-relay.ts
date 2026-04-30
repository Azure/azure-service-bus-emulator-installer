import rhea = require('rhea');
import { ConnectionPool } from './connection-pool';
import { CbsHandler } from './cbs-handler';
import pino from 'pino';

interface LinkMapping {
  clientLink: rhea.Sender | rhea.Receiver;
  backendLink: rhea.Sender | rhea.Receiver;
  backendConnId: string;
  clientConnectionId: string;
  pendingDeliveries: Map<string, rhea.Delivery>;
  backendReady: boolean;
  messageQueue: Array<{ message: rhea.Message; delivery: rhea.Delivery }>;
}

interface ManagementRequest {
  clientDelivery: rhea.Delivery;
  timestamp: number;
}

export class LinkRelay {
  private linkMappings = new Map<string, LinkMapping>();
  private managementRequests = new Map<string, ManagementRequest>();
  private receiveDeliveryMap = new Map<string, { backendDelivery: rhea.Delivery; linkId: string }>();
  private readonly pool: ConnectionPool;
  private readonly cbsHandler: CbsHandler;
  private readonly logger: pino.Logger;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private readonly MANAGEMENT_TTL = 30_000;

  constructor(pool: ConnectionPool, cbsHandler: CbsHandler, logger: pino.Logger) {
    this.pool = pool;
    this.cbsHandler = cbsHandler;
    this.logger = logger.child({ component: 'link-relay' });
    this.sweepTimer = setInterval(() => this.sweepStaleRequests(), 60_000);
  }

  handleSenderOpen(context: rhea.EventContext): void {
    const clientSender = context.sender!;
    const source = clientSender.source as rhea.Source | undefined;
    const target = clientSender.target as rhea.TerminusOptions | undefined;
    const address = source?.address;

    if (this.cbsHandler.handleSenderOpen(context)) return;

    const clientConnectionId = this.getConnectionId(context.connection);
    this.logger.debug({ address, targetAddress: target?.address, clientConnectionId }, 'client receiver opened (proxy sees sender_open)');

    const backend = this.pool.acquire(clientConnectionId);
    if (!backend) {
      clientSender.close({ condition: 'amqp:resource-limit-exceeded', description: 'No backend connections available' });
      return;
    }

    try {
      const receiverOpts: any = {
        source: source ? { address: source.address, filter: source.filter } : address,
        credit_window: 0,
        autoaccept: false,
      };

      if (target?.address) {
        receiverOpts.target = { address: target.address };
      }

      const backendReceiver = backend.conn.open_receiver(receiverOpts);

      const linkId = `link-${clientSender.name}`;
      const mapping: LinkMapping = {
        clientLink: clientSender,
        backendLink: backendReceiver,
        backendConnId: backend.connId,
        clientConnectionId,
        pendingDeliveries: new Map(),
        backendReady: false,
        messageQueue: [],
      };
      this.linkMappings.set(linkId, mapping);

      backendReceiver.on('receiver_open', () => {
        this.logger.debug({ linkId, address }, 'backend receiver opened, adding credit');
        mapping.backendReady = true;
        backendReceiver.add_credit(10);
      });

      backendReceiver.on('receiver_error', (ctx) => {
        this.logger.error({ linkId, address, error: ctx.receiver?.error }, 'backend receiver error');
      });

      backendReceiver.on('message', (ctx) => {
        try {
          this.forwardToClient(linkId, ctx);
        } catch (err) {
          this.logger.error({ err, linkId }, 'error forwarding message to client');
        }
      });

      backendReceiver.on('receiver_close', () => {
        this.cleanupLink(linkId);
      });

      clientSender.on('sender_close', () => {
        this.detachBackendLink(linkId);
      });

      clientSender.on('sendable', () => {
        const m = this.linkMappings.get(linkId);
        if (m && m.backendReady) {
          const br = m.backendLink as rhea.Receiver;
          br.add_credit(10);
        }
      });

    } catch (err) {
      this.logger.error({ err, address }, 'failed to create backend receiver');
      clientSender.close({ condition: 'amqp:internal-error', description: 'Failed to connect to backend' });
      this.pool.decrementLinkCount(backend.connId);
    }
  }

  handleReceiverOpen(context: rhea.EventContext): void {
    const clientReceiver = context.receiver!;
    const target = clientReceiver.target as rhea.TerminusOptions | undefined;
    const source = clientReceiver.source as rhea.Source | undefined;
    const address = target?.address;

    if (this.cbsHandler.handleReceiverOpen(context)) return;

    const clientConnectionId = this.getConnectionId(context.connection);
    const isManagement = address?.includes('$management') ?? false;
    this.logger.debug({ address, sourceAddress: source?.address, isManagement, clientConnectionId }, 'client sender opened (proxy sees receiver_open)');

    const backend = this.pool.acquire(clientConnectionId);
    if (!backend) {
      clientReceiver.close({ condition: 'amqp:resource-limit-exceeded', description: 'No backend connections available' });
      return;
    }

    try {
      const senderOpts: any = {
        target: address,
        autosettle: false,
      };

      if (source?.address) {
        senderOpts.source = { address: source.address };
      }

      const backendSender = backend.conn.open_sender(senderOpts);

      const linkId = `link-${clientReceiver.name}`;
      const mapping: LinkMapping = {
        clientLink: clientReceiver,
        backendLink: backendSender,
        backendConnId: backend.connId,
        clientConnectionId,
        pendingDeliveries: new Map(),
        backendReady: false,
        messageQueue: [],
      };
      this.linkMappings.set(linkId, mapping);

      if (isManagement) {
        this.setupManagementRelay(linkId, mapping, context.connection);
      }

      backendSender.on('sendable', () => {
        this.logger.debug({ linkId, address }, 'backend sender is sendable');
        const m = this.linkMappings.get(linkId);
        if (m) {
          m.backendReady = true;
          this.flushMessageQueue(linkId);
        }
      });

      backendSender.on('sender_open', () => {
        this.logger.debug({ linkId, address }, 'backend sender opened');
      });

      backendSender.on('sender_error', (ctx) => {
        this.logger.error({ linkId, address, error: ctx.sender?.error }, 'backend sender error');
      });

      backendSender.on('sender_close', () => {
        this.logger.debug({ linkId, address }, 'backend sender closed');
        this.cleanupLink(linkId);
      });

      backendSender.on('accepted', (ctx) => {
        try { this.settleClientDelivery(linkId, ctx, 'accept'); } catch (err) { this.logger.error({ err, linkId }, 'settlement error'); }
      });
      backendSender.on('rejected', (ctx) => {
        try { this.settleClientDelivery(linkId, ctx, 'reject'); } catch (err) { this.logger.error({ err, linkId }, 'settlement error'); }
      });
      backendSender.on('released', (ctx) => {
        try { this.settleClientDelivery(linkId, ctx, 'release'); } catch (err) { this.logger.error({ err, linkId }, 'settlement error'); }
      });
      backendSender.on('modified', (ctx) => {
        try { this.settleClientDelivery(linkId, ctx, 'modified'); } catch (err) { this.logger.error({ err, linkId }, 'settlement error'); }
      });

      backendSender.on('sender_close', () => {
        this.cleanupLink(linkId);
      });

      clientReceiver.on('receiver_close', () => {
        this.drainAndDetach(linkId);
      });

    } catch (err) {
      this.logger.error({ err, address }, 'failed to create backend sender');
      clientReceiver.close({ condition: 'amqp:internal-error', description: 'Failed to connect to backend' });
      this.pool.decrementLinkCount(backend.connId);
    }
  }

  handleMessage(context: rhea.EventContext): void {
    if (this.cbsHandler.handleMessage(context)) return;

    const clientReceiver = context.receiver!;
    const linkId = this.findLinkIdByClientLink(clientReceiver);
    this.logger.debug({ receiverName: clientReceiver.name, linkId, totalMappings: this.linkMappings.size }, 'handleMessage lookup');
    if (!linkId) {
      this.logger.warn({ receiverName: clientReceiver.name, mappedLinks: Array.from(this.linkMappings.keys()) }, 'received message on unknown link');
      context.delivery?.reject({ condition: 'amqp:internal-error' });
      return;
    }

    const mapping = this.linkMappings.get(linkId)!;
    const msg = context.message!;

    if (!mapping.backendReady) {
      this.logger.debug({ linkId }, 'backend not ready, queueing message');
      mapping.messageQueue.push({ message: msg, delivery: context.delivery! });
      return;
    }

    this.forwardToBackend(linkId, mapping, msg, context.delivery!);
  }

  handleSettled(context: rhea.EventContext): void {
    const delivery = context.delivery!;

    const deliveryTag = delivery.tag?.toString() ?? `${delivery.id}`;
    const receiveEntry = this.receiveDeliveryMap.get(deliveryTag);
    if (receiveEntry) {
      const state = delivery.remote_state;
      if (state) {
        receiveEntry.backendDelivery.update(true, state);
      } else if (delivery.remote_settled) {
        receiveEntry.backendDelivery.accept();
      }
      this.receiveDeliveryMap.delete(deliveryTag);
      this.logger.debug({ deliveryTag }, 'receive settlement forwarded to backend');
      return;
    }

    const link = delivery.link;
    const linkId = this.findLinkIdByClientLink(link as rhea.Sender | rhea.Receiver);
    if (!linkId) return;

    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    const pendingKey = this.findPendingDeliveryKey(mapping, delivery);
    if (pendingKey) {
      const clientDelivery = mapping.pendingDeliveries.get(pendingKey);
      if (clientDelivery) {
        const state = delivery.remote_state;
        if (state) {
          clientDelivery.update(true, state);
        } else {
          clientDelivery.accept();
        }
        mapping.pendingDeliveries.delete(pendingKey);
      }
    }
  }

  cleanupConnection(connectionId: string): void {
    const toRemove: string[] = [];
    for (const [linkId, mapping] of this.linkMappings) {
      if (mapping.clientConnectionId === connectionId) {
        toRemove.push(linkId);
      }
    }

    for (const linkId of toRemove) {
      this.detachBackendLink(linkId);
    }

    for (const [tag, entry] of this.receiveDeliveryMap) {
      if (toRemove.some(id => id === entry.linkId)) {
        this.receiveDeliveryMap.delete(tag);
      }
    }

    this.cbsHandler.cleanupConnection(connectionId);
    this.pool.releaseClientLinks(connectionId);
    this.logger.debug({ connectionId, linksRemoved: toRemove.length }, 'cleaned up connection');
  }

  shutdown(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }

    for (const [linkId, mapping] of this.linkMappings) {
      try {
        if (mapping.backendLink.is_open()) {
          mapping.backendLink.close();
        }
      } catch {
        // ignore during shutdown
      }
    }
    this.linkMappings.clear();
    this.managementRequests.clear();
  }

  private forwardToBackend(linkId: string, mapping: LinkMapping, msg: rhea.Message, clientDelivery: rhea.Delivery): void {
    const backendSender = mapping.backendLink as rhea.Sender;

    if (!backendSender.sendable()) {
      this.logger.debug({ linkId }, 'backend sender not sendable, queueing');
      mapping.messageQueue.push({ message: msg, delivery: clientDelivery });
      return;
    }

    try {
      const backendDelivery = backendSender.send(msg);
      const deliveryKey = `${backendDelivery.id}`;
      mapping.pendingDeliveries.set(deliveryKey, clientDelivery);
      this.logger.debug({ linkId, deliveryKey, body: typeof msg.body === 'string' ? msg.body.substring(0, 50) : '(binary)' }, 'forwarded message to backend');
    } catch (err) {
      this.logger.error({ err, linkId }, 'failed to forward message to backend');
      clientDelivery.reject({ condition: 'amqp:internal-error' });
    }
  }

  private flushMessageQueue(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    this.logger.debug({ linkId, found: !!mapping, queueLength: mapping?.messageQueue.length ?? -1 }, 'flushMessageQueue called');
    if (!mapping || mapping.messageQueue.length === 0) return;

    this.logger.debug({ linkId, queued: mapping.messageQueue.length }, 'flushing queued messages');
    const queue = [...mapping.messageQueue];
    mapping.messageQueue = [];

    for (const { message, delivery } of queue) {
      this.forwardToBackend(linkId, mapping, message, delivery);
    }
  }

  private forwardToClient(linkId: string, context: rhea.EventContext): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    const clientSender = mapping.clientLink as rhea.Sender;
    if (!clientSender.sendable()) {
      this.logger.debug({ linkId }, 'client sender not ready, releasing message back');
      context.delivery?.release();
      return;
    }

    const msg = context.message!;
    const backendDelivery = context.delivery!;

    this.logger.debug({ linkId, body: typeof msg.body === 'string' ? msg.body.substring(0, 50) : '(binary)' }, 'forwarding message to client');

    const clientDelivery = clientSender.send(msg);

    const onSettled = (settleContext: rhea.EventContext) => {
      const d = settleContext.delivery;
      if (d && d.id === clientDelivery.id) {
        if (d.remote_settled) {
          const state = d.remote_state;
          if (state) {
            backendDelivery.update(true, state);
          } else {
            backendDelivery.accept();
          }
          this.logger.debug({ linkId, deliveryId: d.id }, 'receive settlement forwarded to backend');
        }
        clientSender.removeListener('accepted', onAccepted);
        clientSender.removeListener('released', onReleased);
        clientSender.removeListener('rejected', onRejected);
        clientSender.removeListener('settled', onSettled);
      }
    };

    const onAccepted = (ctx: rhea.EventContext) => {
      if (ctx.delivery && ctx.delivery.id === clientDelivery.id) {
        backendDelivery.accept();
        this.logger.debug({ linkId, deliveryId: ctx.delivery.id }, 'client accepted, forwarded to backend');
        clientSender.removeListener('accepted', onAccepted);
        clientSender.removeListener('released', onReleased);
        clientSender.removeListener('rejected', onRejected);
        clientSender.removeListener('settled', onSettled);
      }
    };

    const onReleased = (ctx: rhea.EventContext) => {
      if (ctx.delivery && ctx.delivery.id === clientDelivery.id) {
        backendDelivery.release();
        clientSender.removeListener('accepted', onAccepted);
        clientSender.removeListener('released', onReleased);
        clientSender.removeListener('rejected', onRejected);
        clientSender.removeListener('settled', onSettled);
      }
    };

    const onRejected = (ctx: rhea.EventContext) => {
      if (ctx.delivery && ctx.delivery.id === clientDelivery.id) {
        backendDelivery.reject(ctx.delivery.remote_state as any);
        clientSender.removeListener('accepted', onAccepted);
        clientSender.removeListener('released', onReleased);
        clientSender.removeListener('rejected', onRejected);
        clientSender.removeListener('settled', onSettled);
      }
    };

    clientSender.on('accepted', onAccepted);
    clientSender.on('released', onReleased);
    clientSender.on('rejected', onRejected);
    clientSender.on('settled', onSettled);

    const backendReceiver = mapping.backendLink as rhea.Receiver;
    backendReceiver.add_credit(1);
  }

  handleAccepted(context: rhea.EventContext): void {
    this.handleClientDisposition(context, 'accept');
  }

  handleRejected(context: rhea.EventContext): void {
    this.handleClientDisposition(context, 'reject');
  }

  handleReleased(context: rhea.EventContext): void {
    this.handleClientDisposition(context, 'release');
  }

  handleModified(context: rhea.EventContext): void {
    this.handleClientDisposition(context, 'modified');
  }

  private handleClientDisposition(context: rhea.EventContext, outcome: string): void {
    const delivery = context.delivery!;
    const deliveryTag = delivery.tag?.toString() ?? `${delivery.id}`;
    const entry = this.receiveDeliveryMap.get(deliveryTag);

    if (entry) {
      switch (outcome) {
        case 'accept': entry.backendDelivery.accept(); break;
        case 'reject': entry.backendDelivery.reject(delivery.remote_state as any); break;
        case 'release': entry.backendDelivery.release(); break;
        case 'modified': entry.backendDelivery.modified(delivery.remote_state as any); break;
      }
      this.receiveDeliveryMap.delete(deliveryTag);
      this.logger.debug({ deliveryTag, outcome }, 'receive settlement forwarded to backend');
    }
  }

  private setupManagementRelay(linkId: string, mapping: LinkMapping, clientConnection: rhea.Connection): void {
    this.logger.debug({ linkId }, 'setting up $management relay');
  }

  private settleClientDelivery(linkId: string, context: rhea.EventContext, outcome: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    const backendDelivery = context.delivery!;
    const deliveryKey = `${backendDelivery.id}`;
    const clientDelivery = mapping.pendingDeliveries.get(deliveryKey);

    if (clientDelivery) {
      switch (outcome) {
        case 'accept': clientDelivery.accept(); break;
        case 'reject': clientDelivery.reject(backendDelivery.remote_state as any); break;
        case 'release': clientDelivery.release(); break;
        case 'modified': clientDelivery.modified(backendDelivery.remote_state as any); break;
      }
      mapping.pendingDeliveries.delete(deliveryKey);
    }
  }

  private drainAndDetach(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    if (mapping.messageQueue.length > 0 || mapping.pendingDeliveries.size > 0) {
      this.logger.debug({ linkId, queued: mapping.messageQueue.length, pending: mapping.pendingDeliveries.size }, 'draining before detach');

      const checkDrained = () => {
        const m = this.linkMappings.get(linkId);
        if (!m || (m.messageQueue.length === 0 && m.pendingDeliveries.size === 0)) {
          this.detachBackendLink(linkId);
        } else {
          setTimeout(checkDrained, 100);
        }
      };

      if (mapping.backendReady) {
        this.flushMessageQueue(linkId);
      }

      setTimeout(checkDrained, 200);
      setTimeout(() => this.detachBackendLink(linkId), 5000);
      return;
    }

    this.detachBackendLink(linkId);
  }

  private detachBackendLink(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    try {
      if (mapping.backendLink.is_open()) {
        mapping.backendLink.close();
      }
    } catch {
      // ignore close errors
    }

    this.pool.decrementLinkCount(mapping.backendConnId);
    this.linkMappings.delete(linkId);
  }

  private cleanupLink(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    try {
      if (mapping.clientLink.is_open()) {
        mapping.clientLink.close();
      }
    } catch {
      // ignore
    }

    this.pool.decrementLinkCount(mapping.backendConnId);
    this.linkMappings.delete(linkId);
  }

  private findLinkIdByClientLink(link: rhea.Sender | rhea.Receiver): string | null {
    for (const [linkId, mapping] of this.linkMappings) {
      if (mapping.clientLink === link || mapping.clientLink.name === link.name) {
        return linkId;
      }
    }
    return null;
  }

  private findPendingDeliveryKey(mapping: LinkMapping, delivery: rhea.Delivery): string | null {
    for (const [key, d] of mapping.pendingDeliveries) {
      if (d === delivery) return key;
    }
    return null;
  }

  private sweepStaleRequests(): void {
    const now = Date.now();
    let swept = 0;
    for (const [id, req] of this.managementRequests) {
      if (now - req.timestamp > this.MANAGEMENT_TTL) {
        this.managementRequests.delete(id);
        swept++;
      }
    }
    if (swept > 0) {
      this.logger.debug({ swept }, 'swept stale management requests');
    }
  }

  private getConnectionId(conn: rhea.Connection): string {
    return (conn as any).__proxyId ?? (conn as any).options?.id ?? (conn as any).id ?? `conn-${conn.container_id}`;
  }
}
