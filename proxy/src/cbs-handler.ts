import rhea = require('rhea');
import pino from 'pino';

const CBS_ADDRESS = '$cbs';

export class CbsHandler {
  private readonly logger: pino.Logger;
  private cbsLinks = new Map<string, { sender: rhea.Sender; receiver: rhea.Receiver }>();

  constructor(logger: pino.Logger) {
    this.logger = logger.child({ component: 'cbs-handler' });
  }

  isCbsAddress(address: string | undefined): boolean {
    return address === CBS_ADDRESS;
  }

  handleSenderOpen(context: rhea.EventContext): boolean {
    const sender = context.sender!;
    const source = sender.source as any;
    if (!this.isCbsAddress(source?.address)) return false;

    const connId = (context.connection as any).options?.id ?? 'unknown';
    this.logger.debug({ connId }, 'CBS sender opened (client receiver for $cbs responses)');

    let entry = this.cbsLinks.get(connId);
    if (!entry) {
      entry = { sender: sender, receiver: undefined as any };
      this.cbsLinks.set(connId, entry);
    } else {
      entry.sender = sender;
    }

    return true;
  }

  handleReceiverOpen(context: rhea.EventContext): boolean {
    const receiver = context.receiver!;
    const target = receiver.target as any;
    if (!this.isCbsAddress(target?.address)) return false;

    const connId = (context.connection as any).options?.id ?? 'unknown';
    this.logger.debug({ connId }, 'CBS receiver opened (client sender for $cbs requests)');

    let entry = this.cbsLinks.get(connId);
    if (!entry) {
      entry = { sender: undefined as any, receiver };
      this.cbsLinks.set(connId, entry);
    } else {
      entry.receiver = receiver;
    }

    return true;
  }

  handleMessage(context: rhea.EventContext): boolean {
    const receiver = context.receiver!;
    const target = receiver.target as any;
    if (!this.isCbsAddress(target?.address)) return false;

    const message = context.message!;
    const operation = message.application_properties?.operation;

    if (operation === 'put-token') {
      this.logger.debug(
        { audience: message.application_properties?.name },
        'CBS put-token request — returning success (auth bypassed)'
      );

      const connId = (context.connection as any).options?.id ?? 'unknown';
      const entry = this.cbsLinks.get(connId);
      if (entry?.sender) {
        entry.sender.send({
          correlation_id: message.message_id,
          to: message.reply_to,
          application_properties: {
            'status-code': 200,
            'status-description': 'OK',
          },
          body: 'OK',
        });
      }

      context.delivery?.accept();
    } else {
      this.logger.warn({ operation }, 'unknown CBS operation');
      context.delivery?.accept();
    }

    return true;
  }

  cleanupConnection(connId: string): void {
    this.cbsLinks.delete(connId);
  }
}
