import { z } from 'zod';

const configSchema = z.object({
  backendHost: z.string().min(1).default('sb-emulator'),
  backendPort: z.coerce.number().int().min(1).max(65535).default(5672),
  maxBackendConnections: z.coerce.number().int().min(1).max(100).default(8),
  proxyPort: z.coerce.number().int().min(1).max(65535).default(5672),
  maxClientConnections: z.coerce.number().int().min(1).max(10000).default(100),
  maxFrameSize: z.coerce.number().int().min(512).default(1048576),
  logLevel: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
});

export type Config = z.infer<typeof configSchema>;

export function loadConfig(): Config {
  const result = configSchema.safeParse({
    backendHost: process.env.BACKEND_HOST,
    backendPort: process.env.BACKEND_PORT,
    maxBackendConnections: process.env.MAX_BACKEND_CONNECTIONS,
    proxyPort: process.env.PROXY_PORT,
    maxClientConnections: process.env.MAX_CLIENT_CONNECTIONS,
    maxFrameSize: process.env.MAX_FRAME_SIZE,
    logLevel: process.env.LOG_LEVEL,
  });

  if (!result.success) {
    console.error('Invalid configuration:');
    for (const issue of result.error.issues) {
      console.error(`  ${issue.path.join('.')}: ${issue.message}`);
    }
    process.exit(1);
  }

  return result.data;
}
