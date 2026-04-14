import { z } from 'zod';

export const tickMessageSchema = z.object({
  type: z.literal('tick'),
  symbol: z.string().min(1),
  ts: z.string().min(1), // ISO
  price: z.number(),
  bid: z.number(),
  ask: z.number(),
  volume: z.number(),
});

export const equityMessageSchema = z.object({
  type: z.literal('equity'),
  ts: z.string().min(1), // ISO
  equity: z.number(),
  pnl: z.number(),
  dd: z.number(),
});

export const positionMessageSchema = z.object({
  type: z.literal('position'),
  ts: z.string().min(1), // ISO
  symbol: z.string().min(1),
  qty: z.number(),
  avgPrice: z.number(),
  unrealizedPnl: z.number(),
});

export const orderMessageSchema = z.object({
  type: z.literal('order'),
  id: z.string().min(1),
  ts: z.string().min(1), // ISO
  symbol: z.string().min(1),
  side: z.enum(['buy', 'sell']),
  orderType: z.enum(['market', 'limit']),
  qty: z.number(),
  price: z.number().optional(),
  filledQty: z.number().default(0),
  status: z.enum(['new', 'partially_filled', 'filled', 'canceled', 'rejected']),
});

export const logMessageSchema = z.object({
  type: z.literal('log'),
  level: z.enum(['info', 'warn', 'error']),
  source: z.string().min(1),
  ts: z.string().min(1), // ISO
  message: z.string(),
});

export const wsMessageSchema = z.discriminatedUnion('type', [
  tickMessageSchema,
  equityMessageSchema,
  positionMessageSchema,
  orderMessageSchema,
  logMessageSchema,
]);

export type TickMessage = z.infer<typeof tickMessageSchema>;
export type EquityMessage = z.infer<typeof equityMessageSchema>;
export type PositionMessage = z.infer<typeof positionMessageSchema>;
export type OrderMessage = z.infer<typeof orderMessageSchema>;
export type LogMessage = z.infer<typeof logMessageSchema>;
export type WsMessage = z.infer<typeof wsMessageSchema>;

