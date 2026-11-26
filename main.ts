// Entry point: speak agent-fun/replay/v2 on stdin/stdout.
//
// The harness sends one hello, then one batch per slot, then done. Each
// batch carries that slot's events, marks, fills and the account; we answer
// with the orders decided on that slot. runBot owns the wire: it keeps the
// state current, echoes each batch's seq, and validates every order before
// it goes out, so a malformed order fails here on our machine, not in
// grading. Diagnostics go to stderr; stdout carries replies only.

import { runBot } from './vendor/makefun-bot.ts';
import { createDipTrader } from './strategy.ts';

const trader = createDipTrader();

await runBot({
  onBatch(state, batch) {
    return trader.onBatch(state, batch);
  },
});
