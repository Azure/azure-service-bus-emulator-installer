import rhea = require('rhea');
import { SessionPool } from './session-pool';
import pino from 'pino';

interface LinkMapping {
  clientLink: rhea.Sender | rhea.Receiver;
  backendLink: rhea.Sender | rhea.Receiver;
  clientConnectionId: string;
  pendingDeliveries: Map<string, rhea.Delivery>;
  backendReady: boolean;
  messageQueue: Array<{ message: rhea.Message; delivery: rhea.Delivery }>;
}

export class SessionRelay {
  private linkMappings = new Map<string, LinkMapping>();
  private receiveDeliveryMap = new Map<string, { backendDelivery: rhea.Delivery; linkId: string }>();
  private cbsSenders = new Map<string, rhea.Sender>();
  private readonly pool: SessionPool;
  private readonly logger: pino.Logger;

  constructor(pool: SessionPool, logger: pino.Logger) {
    this.pool = pool;
    this.logger = logger.child({ component: 'session-relay' });
  }

  private sessionReadyPromises = new Map<string, Promise<rhea.Session>>();
  private clientSessionCounter = new Map<string, number>();

  handleSessionOpen(clientConnectionId: string, clientSession: any): void {
    // Each client session gets its own backend session
    const counter = (this.clientSessionCounter.get(clientConnectionId) || 0) + 1;
    this.clientSessionCounter.set(clientConnectionId, counter);
    const sessionKey = `${clientConnectionId}:s${counter}`;
    (clientSession as any).__backendSessionKey = sessionKey;

    const promise = this.pool.acquireSession(sessionKey).catch((err) => {
      this.logger.error({ err, clientConnectionId, sessionKey }, 'failed to create backend session');
      this.sessionReadyPromises.delete(sessionKey);
      return null as any;
    });
    this.sessionReadyPromises.set(sessionKey, promise);
  }

  private getSessionKey(context: rhea.EventContext): string {
    const session = (context as any).session || context.connection;
    const key = (session as any).__backendSessionKey || this.getConnectionId(context.connection);
    if (!(session as any).__backendSessionKey) {
      this.logger.warn({ fallbackKey: key }, 'session has no __backendSessionKey, using connection ID');
    }
    return key;
  }

  private async waitForSession(sessionKey: string): Promise<rhea.Session | null> {
    const promise = this.sessionReadyPromises.get(sessionKey);
    if (promise) return await promise;
    return this.pool.getSession(sessionKey);
  }

  async handleSenderOpen(context: rhea.EventContext): Promise<void> {
    const clientSender = context.sender!;
    const source = clientSender.source as rhea.Source | undefined;
    const address = source?.address;
    const clientConnectionId = this.getConnectionId(context.connection);

    // CBS sender link (proxy sends responses to client) - handle locally
    if (address === '$cbs') {
      this.cbsSenders.set(clientConnectionId, clientSender);
      this.logger.info({ clientConnectionId }, 'CBS sender link opened (local handler)');
      return;
    }

    const sessionKey = this.getSessionKey(context);
    const backendSession = await this.waitForSession(sessionKey);
    if (!backendSession) {
      this.logger.warn({ address, clientConnectionId }, 'no backend session available, closing sender');
      clientSender.close({ condition: 'amqp:resource-limit-exceeded', description: 'No backend session available' });
      return;
    }

    const receiverOpts: any = {
      source: source ? { address: source.address, filter: source.filter } : address,
      credit_window: 0,
      autoaccept: false,
    };

    const target = clientSender.target as rhea.TerminusOptions | undefined;
    if (target?.address) {
      receiverOpts.target = { address: target.address };
    }

    const backendReceiver = backendSession.open_receiver(receiverOpts);
    const linkId = `link-${clientSender.name}`;
    const mapping: LinkMapping = {
      clientLink: clientSender,
      backendLink: backendReceiver,
      clientConnectionId,
      pendingDeliveries: new Map(),
      backendReady: false,
      messageQueue: [],
    };
    this.linkMappings.set(linkId, mapping);

    const filterObj = source?.filter as any;
    let hasSessionFilter = false;
    if (filterObj) {
      for (const key of Object.keys(filterObj)) {
        if (key.includes('session-filter')) { hasSessionFilter = true; break; }
      }
    }

    if (hasSessionFilter) {
      const senderAny = clientSender as any;
      senderAny._process = function() {};

      backendReceiver.on('receiver_open', () => {
        mapping.backendReady = true;
        const remoteSource = (backendReceiver as any).remote?.attach?.source;
        const remoteProps = (backendReceiver as any).remote?.attach?.properties;

        try {
          if (remoteSource?.address || remoteSource?.filter) {
            senderAny.set_source(remoteSource);
          } else if (source?.address) {
            senderAny.set_source({ address: source.address, filter: source.filter || undefined });
          }
        } catch (err) {
          this.logger.warn({ linkId, err: (err as Error).message }, 'set_source failed');
        }
        if (remoteProps) {
          senderAny.local.attach.properties = remoteProps;
        }

        delete senderAny._process;
        senderAny.connection._register();
        backendReceiver.add_credit(10);
      });

      backendReceiver.on('receiver_error', (ctx) => {
        const error = ctx.receiver?.error;
        delete senderAny._process;
        senderAny.connection._register();
        setTimeout(() => { try { if (clientSender.is_open()) clientSender.close(error); } catch {} }, 10);
      });
    } else {
      backendReceiver.on('receiver_open', () => {
        mapping.backendReady = true;
        backendReceiver.add_credit(10);
      });
    }

    backendReceiver.on('receiver_error', (ctx) => {
      const error = ctx.receiver?.error;
      this.logger.error({ linkId, error }, 'backend receiver error');
      try { if (clientSender.is_open()) clientSender.close(error); } catch {}
    });

    backendReceiver.on('message', (ctx) => {
      try { this.forwardToClient(linkId, ctx); } catch (err) {
        this.logger.error({ err, linkId }, 'error forwarding message to client');
      }
    });

    backendReceiver.on('receiver_close', () => { this.cleanupLink(linkId); });
    clientSender.on('sender_close', () => { this.detachBackendLink(linkId); });

    clientSender.on('sendable', () => {
      const m = this.linkMappings.get(linkId);
      if (m?.backendReady) {
        (m.backendLink as rhea.Receiver).add_credit(10);
      }
    });

    this.logger.debug({ linkId, address, clientConnectionId }, 'sender relay created');
  }

  async handleReceiverOpen(context: rhea.EventContext): Promise<void> {
    const clientReceiver = context.receiver!;
    const target = clientReceiver.target as rhea.TerminusOptions | undefined;
    const address = target?.address;
    const clientConnectionId = this.getConnectionId(context.connection);

    // CBS receiver link (proxy receives put-token requests from client) - handle locally
    if (address === '$cbs') {
      this.logger.info({ clientConnectionId }, 'CBS receiver link opened (local handler)');
      return;
    }

    const sessionKey = this.getSessionKey(context);
    const backendSession = await this.waitForSession(sessionKey);
    if (!backendSession) {
      this.logger.warn({ address, clientConnectionId }, 'no backend session available, closing receiver');
      clientReceiver.close({ condition: 'amqp:resource-limit-exceeded', description: 'No backend session available' });
      return;
    }

    const senderOpts: any = {
      target: address,
      autosettle: false,
    };

    const source = clientReceiver.source as rhea.Source | undefined;
    if (source?.address) {
      senderOpts.source = { address: source.address };
    }

    const backendSender = backendSession.open_sender(senderOpts);
    const linkId = `link-${clientReceiver.name}`;
    const mapping: LinkMapping = {
      clientLink: clientReceiver,
      backendLink: backendSender,
      clientConnectionId,
      pendingDeliveries: new Map(),
      backendReady: false,
      messageQueue: [],
    };
    this.linkMappings.set(linkId, mapping);

    backendSender.on('sendable', () => {
      const m = this.linkMappings.get(linkId);
      if (m) {
        m.backendReady = true;
        this.flushMessageQueue(linkId);
      }
    });

    backendSender.on('sender_error', (ctx) => {
      this.logger.error({ linkId, error: ctx.sender?.error }, 'backend sender error');
    });

    backendSender.on('sender_close', () => { this.cleanupLink(linkId); });

    backendSender.on('accepted', (ctx) => {
      this.settleClientDelivery(linkId, ctx, 'accept');
    });
    backendSender.on('rejected', (ctx) => {
      this.settleClientDelivery(linkId, ctx, 'reject');
    });
    backendSender.on('released', (ctx) => {
      this.settleClientDelivery(linkId, ctx, 'release');
    });
    backendSender.on('modified', (ctx) => {
      this.settleClientDelivery(linkId, ctx, 'modified');
    });

    clientReceiver.on('receiver_close', () => { this.drainAndDetach(linkId); });

    this.logger.debug({ linkId, address, clientConnectionId }, 'receiver relay created');
  }

  handleMessage(context: rhea.EventContext): void {
    const clientReceiver = context.receiver!;
    const target = (clientReceiver.target as any)?.address;

    // Handle CBS messages locally
    if (target === '$cbs') {
      this.handleCbsMessage(context);
      return;
    }

    const linkId = this.findLinkIdByClientLink(clientReceiver);
    if (!linkId) {
      this.logger.warn({ receiverName: clientReceiver.name }, 'received message on unknown link');
      context.delivery?.reject({ condition: 'amqp:internal-error' });
      return;
    }

    const mapping = this.linkMappings.get(linkId)!;
    const msg = context.message!;

    if (!mapping.backendReady) {
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
      if (!delivery.remote_settled) {
        delivery.update(true, state);
      }
      this.receiveDeliveryMap.delete(deliveryTag);
      return;
    }
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

    this.cbsSenders.delete(connectionId);

    // Clean up all sessions for this connection
    const counter = this.clientSessionCounter.get(connectionId) || 0;
    for (let i = 1; i <= counter; i++) {
      const key = `${connectionId}:s${i}`;
      this.sessionReadyPromises.delete(key);
      this.pool.releaseSession(key);
    }
    this.clientSessionCounter.delete(connectionId);
    this.logger.debug({ connectionId, linksRemoved: toRemove.length }, 'cleaned up connection');
  }

  shutdown(): void {
    for (const [, mapping] of this.linkMappings) {
      try { if (mapping.backendLink.is_open()) mapping.backendLink.close(); } catch {}
    }
    this.linkMappings.clear();
  }

  private handleCbsMessage(context: rhea.EventContext): void {
    const message = context.message!;
    const operation = message.application_properties?.operation;
    const clientConnectionId = this.getConnectionId(context.connection);

    if (operation === 'put-token') {
      // Accept the request delivery. The SDK's OnResponseMessage requires
      // the response to arrive UNSETTLED (pre-settled responses are ignored).
      // The accept() generates a DISPOSITION that clears the SDK's delivery state.
      if (context.delivery && !context.delivery.remote_settled) {
        context.delivery.accept();
      }

      const sender = this.cbsSenders.get(clientConnectionId);
      if (sender) {
        const response: rhea.Message = {
          correlation_id: message.message_id,
          to: message.reply_to,
          application_properties: {
            'status-code': (rhea as any).types.wrap_int(200),
            'status-description': 'OK',
          },
          body: 'OK',
        };
        sender.send(response);
        this.logger.info({ clientConnectionId, messageId: message.message_id }, 'CBS put-token → 200 OK');
      }
    }
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
      if (!delivery.remote_settled) {
        delivery.update(true, delivery.remote_state);
      }
      this.receiveDeliveryMap.delete(deliveryTag);
    }
  }

  private forwardToBackend(linkId: string, mapping: LinkMapping, msg: rhea.Message, clientDelivery: rhea.Delivery): void {
    const backendSender = mapping.backendLink as rhea.Sender;

    if (!backendSender.sendable()) {
      mapping.messageQueue.push({ message: msg, delivery: clientDelivery });
      return;
    }

    try {
      const backendDelivery = backendSender.send(msg);
      const deliveryKey = `${backendDelivery.id}`;
      mapping.pendingDeliveries.set(deliveryKey, clientDelivery);
    } catch (err) {
      this.logger.error({ err, linkId }, 'failed to forward message to backend');
      clientDelivery.reject({ condition: 'amqp:internal-error' });
    }
  }

  private flushMessageQueue(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping || mapping.messageQueue.length === 0) return;

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
      context.delivery?.release();
      return;
    }

    const msg = context.message!;
    const backendDelivery = context.delivery!;

    this.preserveAnnotationTypes(msg);

    const backendTag = backendDelivery.tag;
    const clientDelivery = clientSender.send(msg, backendTag);
    const deliveryTag = clientDelivery.tag?.toString() ?? `${linkId}:${clientDelivery.id}`;
    this.receiveDeliveryMap.set(deliveryTag, { backendDelivery, linkId });

    const backendReceiver = mapping.backendLink as rhea.Receiver;
    backendReceiver.add_credit(1);
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
      if (mapping.backendReady) this.flushMessageQueue(linkId);
      setTimeout(() => this.detachBackendLink(linkId), 5000);
      return;
    }

    this.detachBackendLink(linkId);
  }

  private detachBackendLink(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    try { if (mapping.backendLink.is_open()) mapping.backendLink.close(); } catch {}
    this.linkMappings.delete(linkId);
  }

  private cleanupLink(linkId: string): void {
    const mapping = this.linkMappings.get(linkId);
    if (!mapping) return;

    try { if (mapping.clientLink.is_open()) mapping.clientLink.close(); } catch {}
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

  private preserveAnnotationTypes(msg: rhea.Message): void {
    const types = (rhea as any).types;
    this.fixAnnotationMap(msg.message_annotations, types);
    this.fixAnnotationMap((msg as any).delivery_annotations, types);
  }

  private fixAnnotationMap(annotations: any, types: any): void {
    if (!annotations) return;
    for (const key of Object.keys(annotations)) {
      const val = annotations[key];
      if (typeof val === 'number' && Number.isInteger(val)) {
        annotations[key] = types.wrap_long(val);
      } else if (Buffer.isBuffer(val) && val.length === 16) {
        annotations[key] = types.wrap_uuid(val);
      } else if (val instanceof Date) {
        annotations[key] = types.wrap_timestamp(val.getTime());
      }
    }
  }

  private getConnectionId(conn: rhea.Connection): string {
    return (conn as any).__proxyId ?? (conn as any).options?.id ?? (conn as any).id ?? `conn-${conn.container_id}`;
  }
}
