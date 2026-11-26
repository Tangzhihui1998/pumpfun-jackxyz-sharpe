// finG — a Sharpe-first dip trader for pump.fun bonding curves.
//
// Idea: buy small dips on mature curves where buyers are stepping back in,
// exit fast on a modest gain, cut losses tight, and never carry much open
// exposure, so every hour of the replay looks like every other hour.
//
// The strategy is a pure function of each batch: no clock, no environment,
// no randomness, no network. The harness hands us a hello, then one batch
// per slot, and we answer each batch with a list of orders.
//
// Wire shape (per the kit SDK): buys carry { side, mint, quote_lamports,
// max_slippage_bps }; sells carry { side, mint, token_amount,
// max_slippage_bps }. runBot in vendor/makefun-bot.ts owns the NDJSON
// framing, the seq echo and the reply size cap.

import type { Batch, BotState, Curve, OrderInput } from './vendor/makefun-bot.ts';

// ---------------------------------------------------------------- tuning
// All numbers below are the locked finG configuration: a dip entry of
// 0.05 SOL, take-profit +20%, stop-loss -5%, a trailing stop that arms at
// +12% and sells on a 5% pullback from the peak, and a 900-slot max hold.

const LAMPORTS_PER_SOL = 1_000_000_000;
const STAKE_LAMPORTS = 50_000_000n; // 0.05 SOL per entry
const MIN_BUY_LAMPORTS = 10_000_000n; // 0.01 SOL: smallest useful round trip
const MAX_OPEN = 5; // guardrail: at most 5 open positions
const MAX_EXPOSURE_LAMPORTS = 300_000_000n; // 0.30 SOL of open cost, total
const MAX_BUYS_PER_BATCH = 2; // at most 2 fresh entries per batch
const MAX_ENTRIES_PER_MINT = 2; // lifetime entries on one mint
const SLIPPAGE_BPS = 10_000; // wide slippage: fills matter more than price

const AGE_MIN_SLOTS = 100; // curve must be at least this old
const AGE_MAX_SLOTS = 9_000; // ... and no older than this
const RESERVE_MIN_SOL = 2; // real SOL reserve window
const RESERVE_MAX_SOL = 80;
const MIN_TRADES = 8; // trades seen on the curve
const MIN_BUYERS = 4; // distinct buyers
const DIP_MIN = 0.08; // price 8%-35% below its 2500-slot high
const DIP_MAX = 0.35;
const DIP_WINDOW_SLOTS = 2_500;
const FLOW_WINDOW = 8; // last 8 trades: >= 4 buys, net inflow >= 0,
const MIN_FLOW_BUYS = 4; // last trade a buy

const TAKE_PROFIT = 1.2; // sell at +20% vs entry price
const STOP_LOSS = 0.95; // sell at -5% vs entry price
const TRAIL_ARM = 1.12; // trailing stop arms at +12%
const TRAIL_GIVEBACK = 0.05; // ... then sells on a 5% pullback from peak
const MAX_HOLD_SLOTS = 900;
const IDLE_SLOTS = 600; // no trade on the mint for this long: exit

const REBUY_COOLDOWN_SLOTS = 600; // after a full exit, wait before re-entry
const REJECT_COOLDOWN_SLOTS = 600; // after a rejected buy, same wait
const SELL_RESERVE_FRACTION = 0.9; // a sell's proceeds may use 90% of the
// real SOL reserve at most; the rest of the position waits for later batches
const SELL_MIN_FRACTION = 0.05; // skip a partial sell smaller than 5% of
// the holding (not worth the tx fee)

// Diversity safety net: if, between slot 70,000 and 105,000 after the first
// batch, fewer than 17 distinct mints have had a filled buy, relax the
// entry (dip floor 6%, 7 trades) so the 20-mint guardrail stays reachable.
const NET_START_SLOTS = 70_000;
const NET_END_SLOTS = 105_000;
const NET_MIN_MINTS = 17;
const NET_DIP_MIN = 0.06;
const NET_MIN_TRADES = 7;

// How long a near-miss mint stays on the re-check list (slots since its
// last trade). Caps free up and the relaxed window opens without new trades,
// so eligible mints deserve a second look even on a quiet batch.
const WATCH_TTL_SLOTS = 2_000;

// ---------------------------------------------------------------- helpers

/** SOL per token from a curve's virtual reserves (the mark price). */
function markPrice(curve: Curve): number {
  const quote = Number(curve.virtual_quote_reserves);
  const tokens = Number(curve.virtual_token_reserves);
  if (tokens <= 0) return 0;
  return quote / tokens;
}

/** Real SOL reserve of a curve, in SOL. */
function realSol(curve: Curve): number {
  return Number(curve.real_quote_reserves) / LAMPORTS_PER_SOL;
}

// ---------------------------------------------------------------- per-mint notes

interface TradeNote {
  slot: number;
  buy: boolean;
  sol: number; // SOL moved by this trade
}

interface PriceNote {
  slot: number;
  price: number; // SOL per token right after this trade
}

/** Everything remembered about one mint, built only from batch events. */
interface MintNote {
  creator: string | null;
  createdSlot: number | null;
  complete: boolean;
  trades: number;
  buyers: Set<string>;
  recent: TradeNote[]; // last FLOW_WINDOW trades, oldest first
  prices: PriceNote[]; // trade prices inside the dip window
  creatorSold: boolean;
  lastTradeSlot: number;
  entries: number; // filled buys, lifetime
  rejectUntil: number; // slot before which no new buy is tried
  rebuyUntil: number; // slot before which no re-entry is tried
}

/** One open position: cost basis and trailing-stop state. */
interface OpenTrade {
  avgPrice: number; // lamports per token, fees included
  peakRatio: number; // best mark / avgPrice seen so far
  openedSlot: number; // slot of the first buy fill
  tokens: bigint;
}

function freshNote(): MintNote {
  return {
    creator: null,
    createdSlot: null,
    complete: false,
    trades: 0,
    buyers: new Set<string>(),
    recent: [],
    prices: [],
    creatorSold: false,
    lastTradeSlot: -1,
    entries: 0,
    rejectUntil: 0,
    rebuyUntil: 0,
  };
}

// ---------------------------------------------------------------- the engine

export class DipTrader {
  readonly #notes = new Map<string, MintNote>();
  readonly #open = new Map<string, OpenTrade>();
  readonly #boughtMints = new Set<string>();
  readonly #watch = new Set<string>();
  #firstSlot: number | null = null;

  #note(mint: string): MintNote {
    let note = this.#notes.get(mint);
    if (note === undefined) {
      note = freshNote();
      this.#notes.set(mint, note);
    }
    return note;
  }

  /** Fold one batch's events into the per-mint notes. */
  #ingest(batch: Batch): void {
    const slot = batch.slot;
    for (const event of batch.events) {
      const note = this.#note(event.mint);
      if (event.event === 'CreateEvent') {
        note.creator = event.creator;
        note.createdSlot = event.slot;
      } else if (event.event === 'TradeEvent') {
        note.trades += 1;
        note.lastTradeSlot = event.slot;
        const sol = Number(BigInt(event.sol_amount)) / LAMPORTS_PER_SOL;
        if (event.is_buy) note.buyers.add(event.user);
        if (note.creator !== null && !event.is_buy && event.user === note.creator) {
          note.creatorSold = true;
        }
        note.recent.push({ slot: event.slot, buy: event.is_buy, sol });
        if (note.recent.length > FLOW_WINDOW) note.recent.shift();
        const price =
          Number(BigInt(event.virtual_quote_reserves)) /
          Number(BigInt(event.virtual_token_reserves));
        if (price > 0) note.prices.push({ slot: event.slot, price });
        // Drop price samples that fell out of the dip window.
        while (note.prices.length > 0 && note.prices[0].slot <= slot - DIP_WINDOW_SLOTS) {
          note.prices.shift();
        }
      } else {
        note.complete = true;
      }
    }
  }

  /** Fold one batch's fills into positions, entries and cooldowns. */
  #settle(batch: Batch, state: BotState): void {
    for (const fill of batch.fills) {
      const note = this.#note(fill.mint);
      if (fill.side === 'buy' && fill.status === 'filled') {
        note.entries += 1;
        this.#boughtMints.add(fill.mint);
        const paid = Number(BigInt(fill.quote_lamports));
        const got = BigInt(fill.token_amount);
        const prev = this.#open.get(fill.mint);
        if (prev === undefined) {
          this.#open.set(fill.mint, {
            avgPrice: got > 0n ? paid / Number(got) : 0,
            peakRatio: 1,
            openedSlot: fill.slot,
            tokens: got,
          });
        } else {
          const oldTokens = Number(prev.tokens);
          const newTokens = Number(prev.tokens + got);
          prev.avgPrice =
            newTokens > 0 ? (prev.avgPrice * oldTokens + paid) / newTokens : prev.avgPrice;
          prev.tokens += got;
        }
      } else if (fill.side === 'buy' && fill.status === 'rejected') {
        // A rejected buy cools this mint off: do not hammer it.
        note.rejectUntil = Math.max(note.rejectUntil, fill.decided_slot + REJECT_COOLDOWN_SLOTS);
      } else if (fill.side === 'sell' && fill.status === 'filled') {
        const prev = this.#open.get(fill.mint);
        if (prev !== undefined) {
          prev.tokens -= BigInt(fill.token_amount);
          if (prev.tokens <= 0n) {
            this.#open.delete(fill.mint);
            note.rebuyUntil = Math.max(note.rebuyUntil, fill.slot + REBUY_COOLDOWN_SLOTS);
          }
        }
      }
      // Harness closes (_complete / _flatten / _writeoff) arrive as sell
      // fills too, so a fully closed mint is handled by the branch above.
    }
    // Reconcile with the account: drop tracking for positions the harness
    // closed without a fill we saw, and adopt any position we missed.
    for (const [mint, trade] of this.#open) {
      if (!state.positions.has(mint)) {
        this.#open.delete(mint);
        this.#note(mint).rebuyUntil = Math.max(
          this.#note(mint).rebuyUntil,
          batch.slot + REBUY_COOLDOWN_SLOTS,
        );
      } else {
        void trade;
      }
    }
    for (const [mint, holding] of state.positions) {
      if (!this.#open.has(mint)) {
        const tokens = holding.token_amount;
        this.#open.set(mint, {
          avgPrice: tokens > 0n ? Number(holding.cost_lamports) / Number(tokens) : 0,
          peakRatio: 1,
          openedSlot: batch.slot,
          tokens,
        });
      }
    }
  }

  /** Highest trade price on the mint inside the dip window. Samples are
   * only pruned when a new trade arrives on the mint, so a mint that has
   * gone quiet can still carry a stale high: filter by slot here, at
   * decision time, or a dip measured against an ancient high would read
   * as a fresh buying opportunity. */
  #windowHigh(note: MintNote, slot: number): number {
    let high = 0;
    for (const sample of note.prices) {
      if (sample.slot > slot - DIP_WINDOW_SLOTS && sample.price > high) high = sample.price;
    }
    return high;
  }

  /** Is the diversity safety net active on this slot? */
  #relaxed(slot: number): boolean {
    if (this.#firstSlot === null) return false;
    const elapsed = slot - this.#firstSlot;
    return elapsed >= NET_START_SLOTS && elapsed <= NET_END_SLOTS && this.#boughtMints.size < NET_MIN_MINTS;
  }

  /**
   * Entry screen for one mint. `soft` checks only the conditions that do
   * not depend on caps or cash, so near-misses can join the watch list.
   */
  #entryScreen(
    mint: string,
    note: MintNote,
    curve: Curve,
    slot: number,
    relaxed: boolean,
    soft: boolean,
  ): boolean {
    if (note.complete) return false;
    if (note.createdSlot === null) return false;
    const age = slot - note.createdSlot;
    if (age < AGE_MIN_SLOTS || age > AGE_MAX_SLOTS) return false;
    const reserve = realSol(curve);
    if (reserve < RESERVE_MIN_SOL || reserve > RESERVE_MAX_SOL) return false;
    const minTrades = relaxed ? NET_MIN_TRADES : MIN_TRADES;
    if (note.trades < minTrades) return false;
    if (note.buyers.size < MIN_BUYERS) return false;
    if (note.creatorSold) return false;
    const now = markPrice(curve);
    if (now <= 0) return false;
    const high = this.#windowHigh(note, slot);
    if (high <= 0) return false;
    const dip = (high - now) / high;
    const dipMin = relaxed ? NET_DIP_MIN : DIP_MIN;
    if (dip < dipMin || dip > DIP_MAX) return false;
    if (note.recent.length < FLOW_WINDOW - 1) return false;
    let buys = 0;
    let net = 0;
    for (const trade of note.recent) {
      if (trade.buy) {
        buys += 1;
        net += trade.sol;
      } else {
        net -= trade.sol;
      }
    }
    if (buys < MIN_FLOW_BUYS) return false;
    if (net < 0) return false;
    if (!note.recent[note.recent.length - 1].buy) return false;
    if (soft) return true;
    // Hard gates: cooldowns, per-mint entry budget, already holding.
    if (slot < note.rejectUntil || slot < note.rebuyUntil) return false;
    if (note.entries >= MAX_ENTRIES_PER_MINT) return false;
    if (this.#open.has(mint)) return false;
    return true;
  }

  /**
   * How many tokens of a holding to sell this batch. A sell whose gross
   * proceeds would exceed the curve's real SOL reserve is rejected, so only
   * the part that fits inside 90% of the reserve is sold now; the rest
   * waits for later batches, when other buyers have refilled the reserve.
   * Returns 0n when the sellable part is too small to bother with.
   */
  #sellSize(mint: string, curve: Curve, tokens: bigint): bigint {
    if (tokens <= 0n) return 0n;
    const price = markPrice(curve);
    if (price <= 0) return 0n;
    const proceeds = Number(tokens) * price;
    const budget = SELL_RESERVE_FRACTION * Number(curve.real_quote_reserves);
    if (proceeds <= budget) return tokens;
    const sellable = Math.floor(budget / price);
    if (sellable <= 0) return 0n;
    const part = BigInt(sellable);
    if (part * 20n < tokens) return 0n; // under 5% of the holding: skip
    return part;
  }

  /** Decide the orders for one batch. Exits first, then fresh entries. */
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const slot = batch.slot;
    if (this.#firstSlot === null) this.#firstSlot = slot;
    this.#ingest(batch);
    this.#settle(batch, state);

    const orders: OrderInput[] = [];
    const relaxed = this.#relaxed(slot);

    // --- exits ------------------------------------------------------------
    for (const [mint, trade] of this.#open) {
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      // A sell is already in flight on this mint: its fill lands 2 slots
      // after it was decided, and the account still shows the pre-fill
      // tokens until then. Sending another sell now would size off stale
      // tokens and come back rejected (insufficient_tokens), so wait.
      let sellPending = false;
      for (const p of state.pending.values()) {
        if (p.order.side === 'sell' && p.order.mint === mint) sellPending = true;
      }
      if (sellPending) continue;
      const price = markPrice(curve);
      if (price <= 0 || trade.avgPrice <= 0) continue;
      const ratio = price / trade.avgPrice;
      if (ratio > trade.peakRatio) trade.peakRatio = ratio;
      const trailingArmed = trade.peakRatio >= TRAIL_ARM;
      const trailingHit = trailingArmed && ratio <= trade.peakRatio * (1 - TRAIL_GIVEBACK);
      const note = this.#note(mint);
      const shouldSell =
        ratio >= TAKE_PROFIT ||
        ratio <= STOP_LOSS ||
        trailingHit ||
        slot - trade.openedSlot >= MAX_HOLD_SLOTS ||
        slot - note.lastTradeSlot >= IDLE_SLOTS ||
        note.creatorSold;
      if (!shouldSell) continue;
      const held = state.positions.get(mint);
      if (held === undefined) continue;
      const amount = this.#sellSize(mint, curve, held.token_amount);
      if (amount <= 0n) continue;
      orders.push({ side: 'sell', mint, token_amount: amount, max_slippage_bps: SLIPPAGE_BPS });
    }

    // --- entries ----------------------------------------------------------
    // Candidates: mints that traded in this batch, plus the watch list.
    const candidates = new Set<string>();
    for (const event of batch.events) {
      if (event.event === 'TradeEvent') candidates.add(event.mint);
    }
    for (const mint of this.#watch) candidates.add(mint);
    // Prune the watch list: drop stale mints and ones that no longer look
    // like a dip at all, then re-add today's near-misses below.
    for (const mint of [...this.#watch]) {
      const note = this.#notes.get(mint);
      if (note === undefined || slot - note.lastTradeSlot > WATCH_TTL_SLOTS) {
        this.#watch.delete(mint);
      }
    }

    let buys = 0;
    let exposure = 0n;
    for (const holding of state.positions.values()) exposure += holding.cost_lamports;
    for (const pending of state.pending.values()) {
      if (pending.order.side === 'buy') exposure += BigInt(pending.order.quote_lamports);
    }
    const txFee = state.txFeeLamports();

    for (const mint of candidates) {
      if (buys >= MAX_BUYS_PER_BATCH) break;
      if (state.committedPositionCount() + buys >= MAX_OPEN) break;
      const curve = state.curves.get(mint);
      const note = this.#notes.get(mint);
      if (curve === undefined || note === undefined) continue;
      // A buy already in flight on this mint: wait for its fill first.
      let buyPending = false;
      for (const p of state.pending.values()) {
        if (p.order.side === 'buy' && p.order.mint === mint) buyPending = true;
      }
      if (buyPending) continue;
      if (!this.#entryScreen(mint, note, curve, slot, relaxed, false)) {
        // Near-miss? Keep it on the watch list for a later batch.
        if (this.#entryScreen(mint, note, curve, slot, relaxed, true)) this.#watch.add(mint);
        continue;
      }
      this.#watch.delete(mint);
      let size = STAKE_LAMPORTS;
      const cap = state.maxBuyLamports(mint);
      if (cap < size) size = cap;
      if (size < MIN_BUY_LAMPORTS) continue;
      if (exposure + size > MAX_EXPOSURE_LAMPORTS) continue;
      if (state.freeCashLamports() - BigInt(buys) * (size + txFee) < size + txFee) continue;
      orders.push({ side: 'buy', mint, quote_lamports: size, max_slippage_bps: SLIPPAGE_BPS });
      buys += 1;
      exposure += size;
    }

    return orders;
  }
}

/** Build a fresh trader for one run. */
export function createDipTrader(): DipTrader {
  return new DipTrader();
}
