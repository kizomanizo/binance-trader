// server.js
require("dotenv").config();
const express = require("express");
const WebSocket = require("ws");
const crypto = require("crypto");
const { RSI, SMA } = require("technicalindicators");
const initSqlJs = require("sql.js");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = process.env.APP_PORT || 3000;

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "trades.db");
let db;

// Memory store for user's active Binance spot balances & lot size rules
const availableBalances = { USDT: 0, USDC: 0 };
const fundingFreeCache = { USDT: 0, USDC: 0 };
const lockedBalances = {};
const symbolLotSizes = {}; // Stores stepSize precision for each symbol
const tickerPriceCache = {};
const equityQuoteShapeWarned = new Set();
let loggedLiveStockQuotes = false;
const binanceStockAssets = new Set();

// Default Strategy Config (fallback if database is empty)
let config = {
  rsiOversold: 32,
  rsiOverbought: 70,
  volumeSurgeMultiplier: 1.8,
  tradeAmountUsdt: 5.5,
  cooldownMinutes: 5,
  takeProfitPercent: 1.5,
  stopLossPercent: 2.0,
  takerFeePercent: 0.1,
};

let alertConfig = {
  cryptoTelegramEnabled: true,
  stocksTelegramEnabled: true,
  cryptoAllowedStart: "",
  cryptoAllowedEnd: "",
  stocksAllowedStart: "",
  stocksAllowedEnd: "",
};

let botConfig = {
  defaultBudgetUsdt: 20,
  alertProfitPercent: 3,
  alertLossPercent: 3,
};

let botSession = createIdleBotSession();
let botBankroll = createIdleBotBankroll();

function createIdleBotBankroll() {
  return { seedUsdt: 0, cashUsdt: 0 };
}

function createIdleBotSession() {
  return {
    enabled: false,
    startedAt: null,
    budgetUsdt: 0,
    alertProfitPercent: botConfig.alertProfitPercent,
    alertLossPercent: botConfig.alertLossPercent,
    startingEquity: 0,
    cashUsdt: 0,
    positions: {},
    lastAction: "Idle",
    lastAlertKind: null,
    lastAlertAt: 0,
    realizedPnl: 0,
    busy: false,
    halted: false,
    haltReason: "",
    userStopped: false,
    deskGeneration: 0,
    floorUsdt: 0,
  };
}

function saveDatabase() {
  if (!db) return;
  const data = db.export();
  fs.writeFileSync(DB_PATH, Buffer.from(data));
}

async function initDatabase() {
  const SQL = await initSqlJs();

  if (fs.existsSync(DB_PATH)) {
    db = new SQL.Database(fs.readFileSync(DB_PATH));
    console.log("Loaded existing database from disk.");
  } else {
    db = new SQL.Database();
    console.log("Created fresh SQLite database.");
  }

  db.run(`
    CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      side TEXT NOT NULL,
      price REAL NOT NULL,
      qty REAL NOT NULL,
      usdt_amount REAL NOT NULL,
      order_id TEXT NOT NULL,
      timestamp INTEGER NOT NULL
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS strategy_alerts (
      id TEXT PRIMARY KEY,
      symbol TEXT NOT NULL,
      action TEXT NOT NULL,
      signal_type TEXT NOT NULL,
      price REAL NOT NULL,
      rsi REAL,
      created_time INTEGER NOT NULL,
      executed INTEGER NOT NULL DEFAULT 0,
      order_id TEXT,
      source TEXT NOT NULL DEFAULT 'crypto'
    )
  `);

  ensureColumn("strategy_alerts", "source", "TEXT NOT NULL DEFAULT 'crypto'");
  ensureColumn("trades", "source", "TEXT NOT NULL DEFAULT 'manual'");
  loadSettingsFromDb();
  repairTradeTimestamps();
  saveDatabase();
}

function repairTradeTimestamps() {
  const stmt = db.prepare("SELECT id FROM trades WHERE timestamp > 0 AND timestamp < 1000000000000");
  const ids = [];
  while (stmt.step()) ids.push(stmt.getAsObject().id);
  stmt.free();
  ids.forEach((id) => db.run("UPDATE trades SET timestamp = timestamp * 1000 WHERE id = ?", [id]));
  if (ids.length) console.log(`[TRADES] Repaired ${ids.length} second-based timestamps.`);
}

function ensureColumn(table, column, type) {
  const stmt = db.prepare(`PRAGMA table_info(${table})`);
  let exists = false;
  while (stmt.step()) {
    if (stmt.getAsObject().name === column) exists = true;
  }
  stmt.free();
  if (!exists) db.run(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
}

function parseStoredAlertValue(key, value) {
  if (key.endsWith("Enabled")) return value === "true" || value === "1";
  return value == null ? "" : String(value);
}

function loadSettingsFromDb() {
  try {
    const stmt = db.prepare("SELECT key, value FROM settings");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.key in config) {
        config[row.key] = parseFloat(row.value);
      } else if (row.key in alertConfig) {
        alertConfig[row.key] = parseStoredAlertValue(row.key, row.value);
      } else if (row.key in botConfig) {
        const n = parseFloat(row.value);
        if (Number.isFinite(n)) botConfig[row.key] = n;
      } else if (row.key === "botSession") {
        restoreBotSession(row.value);
      } else if (row.key === "botBankroll") {
        restoreBotBankroll(row.value);
      }
    }
    stmt.free();
    seedBankrollFromSession();
    console.log("Loaded strategy config from DB:", config);
    console.log("Loaded alert config from DB:", alertConfig);
    console.log("Loaded bot config from DB:", botConfig);
  } catch (err) {
    console.error("Error loading settings from DB:", err?.message || err);
  }
}

function saveSettingToDb(key, value) {
  db.run(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`, [key, String(value)]);
  saveDatabase();
}

const ALERT_RETENTION = 50;

function recordStrategyAlert({ symbol, action, signalType, price, rsi, source = "crypto" }) {
  if (!db) return null;
  const createdTime = Date.now();
  const id = `${String(symbol).toUpperCase()}_${action}_${createdTime}`;
  db.run(`INSERT INTO strategy_alerts (id, symbol, action, signal_type, price, rsi, created_time, executed, source) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)`, [
    id,
    String(symbol).toUpperCase(),
    action,
    signalType || action,
    parseFloat(price) || 0,
    typeof rsi === "number" ? rsi : null,
    createdTime,
    source === "stocks" ? "stocks" : "crypto",
  ]);

  const stmt = db.prepare("SELECT id FROM strategy_alerts ORDER BY created_time DESC");
  const ids = [];
  while (stmt.step()) ids.push(stmt.getAsObject().id);
  stmt.free();
  ids.slice(ALERT_RETENTION).forEach((oldId) => {
    db.run("DELETE FROM strategy_alerts WHERE id = ?", [oldId]);
  });
  saveDatabase();
  return id;
}

function listStrategyAlerts() {
  if (!db) return [];
  const stmt = db.prepare("SELECT * FROM strategy_alerts ORDER BY created_time DESC");
  const alerts = [];
  while (stmt.step()) {
    const row = stmt.getAsObject();
    alerts.push({
      id: row.id,
      symbol: row.symbol,
      action: row.action,
      signalType: row.signal_type,
      price: row.price,
      rsi: row.rsi === null || row.rsi === undefined ? null : parseFloat(Number(row.rsi).toFixed(2)),
      createdTime: row.created_time,
      executed: Boolean(row.executed),
      orderId: row.order_id || null,
      source: row.source === "stocks" ? "stocks" : "crypto",
    });
  }
  stmt.free();
  return alerts;
}

function markAlertExecuted(alertId, orderId) {
  if (!db || !alertId) return;
  db.run(`UPDATE strategy_alerts SET executed = 1, order_id = ? WHERE id = ?`, [orderId ? String(orderId) : "", alertId]);
  saveDatabase();
}

function tradeLookupSymbols(symbol) {
  const upper = String(symbol || "").toUpperCase();
  const base = upper.replace(/USDT$|USDC$|USD$/, "");
  const ticker = stockTickerFromAsset(base);
  if (STOCK_SYMBOLS.includes(ticker) || isStockAsset(ticker) || isStockAsset(upper) || isStockAsset(base)) {
    return [...new Set([ticker, `${ticker}USDT`, `${ticker}USD`, `${ticker}USDC`, `${ticker}X`, `${ticker}XUSDT`, `${ticker}B`, `${ticker}BUSDT`, `B${ticker}`, upper, base].filter(Boolean))];
  }
  return [upper];
}

function getAverageEntryPrice(symbol) {
  try {
    const symbols = tradeLookupSymbols(symbol);
    const placeholders = symbols.map(() => "?").join(",");
    const stmt = db.prepare(`SELECT side, qty, usdt_amount FROM trades WHERE UPPER(symbol) IN (${placeholders}) ORDER BY timestamp ASC`);
    stmt.bind(symbols);

    let totalQty = 0;
    let totalCost = 0;

    while (stmt.step()) {
      const trade = stmt.getAsObject();
      if (trade.side === "BUY") {
        totalQty += trade.qty;
        totalCost += trade.usdt_amount;
      } else if (trade.side === "SELL") {
        if (totalQty > 0) {
          const avgPrice = totalCost / totalQty;
          const costBasisForSale = trade.qty * avgPrice;
          totalCost = Math.max(0, totalCost - costBasisForSale);
          totalQty = Math.max(0, totalQty - trade.qty);
        }
      }
    }
    stmt.free();

    return totalQty > 0 ? totalCost / totalQty : null;
  } catch (err) {
    console.error(`Error calculating entry price for ${symbol}:`, err.message);
    return null;
  }
}

function getTakerFeeRate() {
  const pct = parseFloat(config.takerFeePercent);
  return (Number.isFinite(pct) && pct >= 0 ? pct : 0.1) / 100;
}

function roundTripFeePercent() {
  return getTakerFeeRate() * 200;
}

// The coil book was selling 0.6× the 8-hour range, about 0.6–1%, on a 1-minute wick.
// A stop under 3% sits inside that noise. Honor a wider saved stop, never a tighter one.
function clipTargetPercent() {
  const configured = parseFloat(config.takeProfitPercent);
  const target = Number.isFinite(configured) && configured > 0 ? configured : 2;
  return parseFloat(Math.max(0.8, target).toFixed(2));
}

function clipStopPercent() {
  const configured = parseFloat(config.stopLossPercent);
  const stop = Number.isFinite(configured) && configured > 0 ? configured : 3;
  return parseFloat(Math.max(3, stop).toFixed(2));
}

function isFifteenMinuteClose(openTimeMs) {
  const minute = new Date(Number(openTimeMs)).getUTCMinutes();
  return Number.isFinite(minute) && minute % 15 === 14;
}

function effectiveExits() {
  const fee = roundTripFeePercent();
  let takeProfit = clipTargetPercent();
  const minTp = Math.max(fee * 4, 0.8);
  if (takeProfit < minTp) takeProfit = minTp;
  const stopLoss = clipStopPercent();
  const breakevenArm = Math.max(fee * 3, takeProfit * 0.5);
  return {
    takeProfit: parseFloat(takeProfit.toFixed(2)),
    stopLoss: parseFloat(stopLoss.toFixed(2)),
    fee: parseFloat(fee.toFixed(3)),
    breakevenArm: parseFloat(breakevenArm.toFixed(2)),
  };
}

function getAssetUsdPrice(asset) {
  if (!asset || asset === "USDT" || asset === "USD") return 1;
  const upper = String(asset).toUpperCase();
  const tick = marketData[`${upper}USDT`];
  if (tick?.prices?.length) return tick.prices[tick.prices.length - 1];
  if (tickerPriceCache[`${upper}USDT`]?.price) return tickerPriceCache[`${upper}USDT`].price;
  if (tickerPriceCache[upper]?.price) return tickerPriceCache[upper].price;
  const stock = stockMarketData[upper] || stockMarketData[stockTickerFromAsset(upper)];
  if (Number.isFinite(stock?.lastPrice)) return stock.lastPrice;
  return null;
}

function compactAssetName(asset) {
  return String(asset || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function stockAliases(ticker) {
  const t = String(ticker || "").toUpperCase();
  return [t, `${t}X`, `${t}B`, `B${t}`, `EQ${t}`, `1${t}`, `${t}USDT`, `${t}USD`, `${t}USDC`, `${t}XUSDT`];
}

function stockTickerFromAsset(asset) {
  const raw = String(asset || "").toUpperCase();
  const compact = compactAssetName(raw);
  if (!compact) return "";
  if (STOCK_SYMBOLS.includes(raw) || STOCK_SYMBOLS.includes(compact)) return STOCK_SYMBOLS.includes(raw) ? raw : compact;
  for (const ticker of STOCK_SYMBOLS) {
    if (stockAliases(ticker).includes(raw) || stockAliases(ticker).includes(compact)) return ticker;
  }
  const stripped = compact.replace(/^EQ/, "").replace(/^1(?=[A-Z])/, "").replace(/(USDT|USDC|USD)$/, "").replace(/[XB]$/, "");
  if (STOCK_SYMBOLS.includes(stripped)) return stripped;
  if (binanceStockAssets.has(raw) || binanceStockAssets.has(compact) || binanceStockAssets.has(stripped)) {
    return stripped || compact;
  }
  return stripped || compact;
}

function isStockAsset(asset) {
  const a = String(asset || "").toUpperCase();
  if (!a) return false;
  const compact = compactAssetName(a);
  const ticker = stockTickerFromAsset(a);
  if (STOCK_SYMBOLS.includes(ticker)) return true;
  if (binanceStockAssets.has(a) || binanceStockAssets.has(compact) || binanceStockAssets.has(ticker)) return true;
  return compact.startsWith("EQ") && Boolean(ticker);
}

const stockPositionCache = {};

function rememberStockPositions(rows) {
  const next = {};
  for (const row of rows || []) {
    const asset = String(row.asset || "").toUpperCase();
    if (!asset || (row.venue !== "binance-stock" && !isStockAsset(asset))) continue;
    const ticker = stockTickerFromAsset(asset);
    if (!ticker) continue;
    const qty = parseFloat(row.total != null ? row.total : (Number(row.free) || 0) + (Number(row.locked) || 0));
    if (!(qty > 0.0001)) continue;
    next[ticker] = { asset, qty: (next[ticker]?.qty || 0) + qty, at: Date.now() };
  }
  Object.keys(stockPositionCache).forEach((key) => delete stockPositionCache[key]);
  Object.assign(stockPositionCache, next);
}

function getHeldStockQty(ticker) {
  return stockPositionCache[String(ticker || "").toUpperCase()]?.qty || 0;
}

function getNetPnlPercent(avgEntryPrice, closePrice) {
  if (!avgEntryPrice || !closePrice) return null;
  const netExit = closePrice * (1 - getTakerFeeRate());
  return ((netExit - avgEntryPrice) / avgEntryPrice) * 100;
}

function botEntryPrice(pos) {
  const qty = parseFloat(pos?.qty) || 0;
  const cost = parseFloat(pos?.costUsdt) || 0;
  if (qty > 0 && cost > 0) return cost / qty;
  return parseFloat(pos?.entryPrice) || 0;
}

function normalizeEpochMs(value) {
  const n = parseInt(value, 10);
  if (!Number.isFinite(n) || n <= 0) return Date.now();
  if (n < 1e12) return n * 1000;
  return n;
}

function getMinNotional(symbol) {
  return symbolLotSizes[String(symbol || "").toUpperCase()]?.minNotional || 5;
}

function restoreBotSession(raw) {
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;
    botSession = {
      ...createIdleBotSession(),
      ...parsed,
      positions: parsed.positions && typeof parsed.positions === "object" ? parsed.positions : {},
      busy: false,
    };
    if (botSession.enabled) {
      console.log(`[AUTOPILOT] Restored enabled session · cash $${Number(botSession.cashUsdt || 0).toFixed(2)}`);
    }
  } catch (err) {
    console.warn("[AUTOPILOT] Failed to restore session:", err.message);
    botSession = createIdleBotSession();
  }
}

function persistBotSession() {
  const { busy, ...rest } = botSession;
  saveSettingToDb("botSession", JSON.stringify(rest));
}

function restoreBotBankroll(raw) {
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;
    botBankroll = {
      seedUsdt: parseFloat(parsed.seedUsdt) || 0,
      cashUsdt: parseFloat(parsed.cashUsdt) || 0,
    };
  } catch (err) {
    console.warn("[AUTOPILOT] Failed to restore bankroll:", err.message);
    botBankroll = createIdleBotBankroll();
  }
}

function persistBotBankroll() {
  saveSettingToDb("botBankroll", JSON.stringify(botBankroll));
}

function syncBotBankroll() {
  const cash = parseFloat(botSession.cashUsdt) || 0;
  if (!botBankroll.seedUsdt && botSession.startingEquity) {
    botBankroll.seedUsdt = parseFloat(botSession.startingEquity) || cash;
  }
  if (cash || botOpenPositionCount() === 0) botBankroll.cashUsdt = cash;
  persistBotBankroll();
}

function seedBankrollFromSession() {
  if (botBankroll.seedUsdt || botBankroll.cashUsdt) return;
  const cash = parseFloat(botSession.cashUsdt) || 0;
  const seed = parseFloat(botSession.startingEquity) || 0;
  if (cash < 5 && seed < 5) return;
  botBankroll.seedUsdt = seed || cash;
  botBankroll.cashUsdt = cash || seed;
  persistBotBankroll();
}

function markToMarketBotPositions() {
  let value = 0;
  const open = [];
  Object.entries(botSession.positions || {}).forEach(([symbol, pos]) => {
    if (!pos) return;
    const qty = parseFloat(pos.qty) || 0;
    const fallback = parseFloat(pos.entryPrice) || 0;
    const px = marketData[symbol]?.prices?.slice(-1)[0] || fallback;
    const usd = qty * px;
    value += usd;
    open.push({
      symbol,
      qty,
      usd: parseFloat(usd.toFixed(2)),
      entryPrice: fallback,
      orderId: pos.orderId || null,
    });
  });
  return { value, open };
}

let lastSetupNote = "";

function describeBotWatch() {
  const exits = effectiveExits();
  const floor = parseFloat(botSession.floorUsdt) || 0;
  const clip = Number(config.tradeAmountUsdt) || 0;
  const risk = floor > 0 ? ` · clip $${clip.toFixed(2)} · floor $${floor.toFixed(2)}` : "";
  const base = lastSetupNote || "Flat · 8h coil break on BTC, ETH, SOL, BNB, XRP";
  return base + risk;
}

function getBotStatus() {
  const mt = markToMarketBotPositions();
  const cash = parseFloat(botSession.cashUsdt) || parseFloat(botBankroll.cashUsdt) || 0;
  const equity = cash + mt.value;
  const starting = parseFloat(botSession.startingEquity) || 0;
  const seed = parseFloat(botBankroll.seedUsdt) || starting;
  const pnl = botSession.startedAt ? equity - starting : 0;
  const pnlPercent = starting > 0 ? (pnl / starting) * 100 : 0;
  const lifetimePnl = seed > 0 ? equity - seed : pnl;
  let lastAction = botSession.lastAction || "Idle";
  if (botSession.enabled && mt.open.length === 0) {
    lastAction = describeBotWatch();
  }
  return {
    enabled: Boolean(botSession.enabled),
    startedAt: botSession.startedAt,
    budgetUsdt: parseFloat((botSession.budgetUsdt || cash || botConfig.defaultBudgetUsdt).toFixed(2)),
    cashUsdt: parseFloat(cash.toFixed(2)),
    equity: parseFloat(equity.toFixed(2)),
    pnl: parseFloat(pnl.toFixed(2)),
    pnlPercent: parseFloat(pnlPercent.toFixed(2)),
    lifetimePnl: parseFloat(lifetimePnl.toFixed(2)),
    realizedPnl: parseFloat((botSession.realizedPnl || 0).toFixed(2)),
    lastAction,
    positions: mt.open,
    alertProfitPercent: botSession.alertProfitPercent,
    alertLossPercent: botSession.alertLossPercent,
    usdtWallet: parseFloat(stableSpendable("USDT").total.toFixed(2)),
    usdcWallet: parseFloat(stableSpendable("USDC").total.toFixed(2)),
    defaults: { ...botConfig },
    clipUsdt: config.tradeAmountUsdt,
    floorUsdt: parseFloat((parseFloat(botSession.floorUsdt) || 0).toFixed(2)),
    halted: Boolean(botSession.halted),
    haltReason: botSession.haltReason || "",
    stocksEnabled: STOCKS_ENABLED,
    bankroll: {
      seedUsdt: parseFloat((seed || 0).toFixed(2)),
      cashUsdt: parseFloat((parseFloat(botBankroll.cashUsdt) || cash).toFixed(2)),
    },
  };
}

function evaluateBotSellSignal(symbol, closePrice, rsi, extreme) {
  const pos = botSession.positions[symbol];
  if (!pos || !closePrice) return null;
  const entry = botEntryPrice(pos);
  const netPnl = getNetPnlPercent(entry, closePrice);
  if (isPeggedSymbol(symbol)) return { type: "FLATTEN_PEG", avgEntryPrice: entry, netPnl };
  if (!entry || netPnl === null) return null;
  if (!parseFloat(pos.openedAt)) {
    pos.openedAt = parseFloat(botSession.startedAt) || Date.now();
    persistBotSession();
  }

  const prevMax = Number.isFinite(parseFloat(pos.maxNetPnl)) ? parseFloat(pos.maxNetPnl) : -Infinity;
  pos.maxNetPnl = Math.max(prevMax, netPnl);
  if (pos.maxNetPnl > prevMax + 0.05) persistBotSession();

  const stopPct = clipStopPercent();
  const targetPct = clipTargetPercent();
  const closePnl = ((closePrice - entry) / entry) * 100;
  if (closePnl >= targetPct) return { type: "TAKE_PROFIT", avgEntryPrice: entry, netPnl };
  // A 1-minute wick is not a stop. Wait until the 15-minute candle closes through it.
  if (extreme?.fifteenMinuteClose && closePnl <= -stopPct) {
    return { type: "STOP_LOSS", avgEntryPrice: entry, netPnl };
  }
  return null;
}

function maybeSessionPnlAlert() {
  if (!botSession.enabled || !botSession.startedAt) return;
  const snap = getBotStatus();
  const now = Date.now();
  if (now - (botSession.lastAlertAt || 0) < 60 * 1000) return;

  if (snap.pnlPercent >= botSession.alertProfitPercent && botSession.lastAlertKind !== "profit") {
    botSession.lastAlertKind = "profit";
    botSession.lastAlertAt = now;
    persistBotSession();
    sendTelegramAlert(
      `🤖 <b>AUTOPILOT SESSION +${snap.pnlPercent.toFixed(2)}%</b>\n\n` +
        `<b>Session P/L:</b> ${snap.pnl >= 0 ? "+" : ""}$${snap.pnl.toFixed(2)}\n` +
        `<b>Capital:</b> $${snap.equity.toFixed(2)} / $${snap.budgetUsdt.toFixed(2)} budget\n` +
        `<b>Cash:</b> $${snap.cashUsdt.toFixed(2)} USDT\n` +
        `<b>Action:</b> Tighten alerts or turn Autopilot OFF to park in USDT.`,
    );
  } else if (snap.pnlPercent <= -botSession.alertLossPercent && botSession.lastAlertKind !== "loss") {
    botSession.lastAlertKind = "loss";
    botSession.lastAlertAt = now;
    persistBotSession();
    sendTelegramAlert(
      `🤖 <b>AUTOPILOT SESSION ${snap.pnlPercent.toFixed(2)}%</b>\n\n` +
        `<b>Session P/L:</b> $${snap.pnl.toFixed(2)}\n` +
        `<b>Capital:</b> $${snap.equity.toFixed(2)} / $${snap.budgetUsdt.toFixed(2)} budget\n` +
        `<b>Cash:</b> $${snap.cashUsdt.toFixed(2)} USDT\n` +
        `<b>Action:</b> Consider OFF to flatten back to USDT, or raise the loss alert.`,
    );
  }
}

function evaluateSellSignal(symbol, closePrice, rsi) {
  const avgEntryPrice = getAverageEntryPrice(symbol);
  const netPnl = getNetPnlPercent(avgEntryPrice, closePrice);
  const exits = effectiveExits();

  if (avgEntryPrice && netPnl !== null) {
    if (netPnl >= exits.takeProfit) return { type: "TAKE_PROFIT", avgEntryPrice, netPnl };
    if (typeof rsi === "number" && rsi >= config.rsiOverbought && netPnl >= Math.max(exits.fee * 3, 0.4)) {
      return { type: "TAKE_PROFIT", avgEntryPrice, netPnl };
    }
    if (netPnl <= -exits.stopLoss) return { type: "STOP_LOSS", avgEntryPrice, netPnl };
    return null;
  }

  if (typeof rsi === "number" && rsi >= config.rsiOverbought) {
    return { type: "SELL", avgEntryPrice: null, netPnl: null };
  }
  return null;
}

function summarizeOrderFills(result, symbol, side) {
  const baseAsset = pairBaseAsset(symbol);
  const fills = Array.isArray(result.fills) ? result.fills : [];
  let commissionUsdt = 0;
  let baseCommission = 0;

  fills.forEach((fill) => {
    const commission = parseFloat(fill.commission || 0);
    if (!commission) return;
    const asset = fill.commissionAsset;
    if (asset === "USDT" || asset === "USDC") commissionUsdt += commission;
    else if (asset === baseAsset) baseCommission += commission;
    else {
      const px = getAssetUsdPrice(asset);
      if (px) commissionUsdt += commission * px;
    }
  });

  const executedQty = parseFloat(result.executedQty || 0);
  const quoteQty = parseFloat(result.cummulativeQuoteQty || 0);
  const isBuy = side.toUpperCase() === "BUY";

  return {
    executedQty,
    netQty: isBuy ? Math.max(0, executedQty - baseCommission) : executedQty,
    recordedUsdt: isBuy ? quoteQty + commissionUsdt : Math.max(0, quoteQty - commissionUsdt),
    commissionUsdt,
    baseCommission,
  };
}

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const TRADE_PASSWORD = process.env.TRADE_PASSWORD || "admin123";

const SYMBOLS = (process.env.SYMBOLS || "btcusdt,ethusdt,solusdt,dogeusdt,xrpusdt").split(",").map((s) => s.trim().toLowerCase());
const INTERVAL = "1m";
const STOCKS_ENABLED = false;
const DESK_GENERATION = 2;
const DESK_CLIP_CAP_USDT = 40;
const DESK_FLOOR_FRACTION = 0.25;
const BOT_UNIVERSE_SIZE = 25;
const BOT_SCAN_MS = 5 * 60 * 1000;
const BOT_MIN_QUOTE_VOLUME = 3_000_000;
const COIL_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"];
const COIL_BARS = 32;
const COIL_MIN_WIDTH = 0.6;
const COIL_MAX_WIDTH = 1.8;
const COIL_VOL_MULT = 1.25;
const LEVERAGE_USDT_RE = /(UP|DOWN|BULL|BEAR)USDT$/;
const STABLE_USDT_PAIRS = new Set(["USDTUSDT", "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT", "USDPUSDT", "USDEUSDT", "USD1USDT"]);
const PEGGED_BASES = new Set([
  "USDT", "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "USDP", "USDE", "USD1", "RLUSD",
  "PYUSD", "USDD", "FRAX", "LUSD", "GUSD", "USDS", "BFUSD", "XUSD", "USD0", "USDY",
  "EUR", "EURI", "AEUR", "USDP", "SUSD", "GUSD", "USDJ", "USTC", "UST", "USDN",
]);

function pairBaseAsset(symbol) {
  return String(symbol || "").toUpperCase().replace(/(USDT|USDC|FDUSD|BUSD|TUSD|USD)$/, "");
}

function pairQuote(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (sym.endsWith("USDC")) return "USDC";
  return "USDT";
}

function isPeggedSymbol(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (STABLE_USDT_PAIRS.has(sym)) return true;
  const base = pairBaseAsset(sym);
  if (!base || PEGGED_BASES.has(base)) return true;
  if (/USD$/.test(base) && base.length <= 8) return true;
  return false;
}

const marketData = {};
let binanceKlineWs = null;
let binanceWsReconnectTimer = null;
const subscribedKlines = new Set();
const botHuntSymbols = new Set();

function emptyMarketSlot(hunt = false) {
  return {
    prices: [],
    volumes: [],
    lastRsi: null,
    lastVolumeSurge: false,
    lastSignalTime: 0,
    lastStopLossPnl: null,
    hunt: Boolean(hunt),
  };
}

SYMBOLS.forEach((sym) => {
  marketData[sym.toUpperCase()] = emptyMarketSlot(false);
});

function isDashboardSymbol(symbol) {
  return SYMBOLS.includes(String(symbol || "").toLowerCase());
}

function ensureMarketSlot(symbol, hunt = false) {
  const sym = String(symbol || "").toUpperCase();
  if (!marketData[sym]) marketData[sym] = emptyMarketSlot(hunt);
  else if (hunt) marketData[sym].hunt = true;
  return marketData[sym];
}

function isTradableUsdtSpot(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (!sym.endsWith("USDT")) return false;
  if (LEVERAGE_USDT_RE.test(sym) || isPeggedSymbol(sym)) return false;
  return Boolean(symbolLotSizes[sym]);
}

function klineStreamName(symbol) {
  return `${String(symbol).toLowerCase()}@kline_${INTERVAL}`;
}

function huntKlineStreams() {
  const streams = new Set();
  botHuntSymbols.forEach((sym) => {
    if (!isDashboardSymbol(sym)) streams.add(klineStreamName(sym));
  });
  Object.keys(botSession.positions || {}).forEach((sym) => {
    if (isDashboardSymbol(sym)) return;
    botHuntSymbols.add(sym);
    streams.add(klineStreamName(sym));
  });
  return [...streams];
}

function subscribeKline(symbol) {
  const stream = klineStreamName(symbol);
  const isNew = !subscribedKlines.has(stream);
  subscribedKlines.add(stream);
  if (!isDashboardSymbol(symbol)) botHuntSymbols.add(String(symbol).toUpperCase());
  if (!isNew || !binanceKlineWs || binanceKlineWs.readyState !== WebSocket.OPEN) return;
  binanceKlineWs.send(JSON.stringify({ method: "SUBSCRIBE", params: [stream], id: Date.now() }));
  console.log(`[AUTOPILOT] subscribed ${String(symbol).toUpperCase()}`);
}

const STOCK_SYMBOLS = (process.env.STOCK_SYMBOLS || "AAPL,TSLA,NVDA,SPY")
  .split(",")
  .map((s) => s.trim().toUpperCase())
  .filter(Boolean);
const stockMarketData = {};

STOCK_SYMBOLS.forEach((symbol) => {
  stockMarketData[symbol] = {
    prices: [],
    volumes: [],
    lastPrice: null,
    lastBarTime: null,
    lastRsi: null,
    lastVolumeSurge: false,
    lastSignalTime: 0,
    lastSignal: null,
    lastStopLossPnl: null,
    sawLiveBar: false,
    pendingClose: null,
    pendingVolume: null,
  };
});

function isBinanceStockTradeSymbol(symbol) {
  const ticker = stockTickerFromAsset(symbol);
  return STOCK_SYMBOLS.includes(ticker) || isStockAsset(symbol) || isStockAsset(ticker);
}

// Fetch LOT_SIZE rules to format trade quantities precisely for Binance
async function fetchExchangeInfo() {
  try {
    const res = await fetch("https://api.binance.com/api/v3/exchangeInfo");
    const data = await res.json();
    if (data.symbols) {
      data.symbols.forEach((s) => {
        if (s.status && s.status !== "TRADING") return;
        if (s.isSpotTradingAllowed === false) return;
        const lotFilter = s.filters.find((f) => f.filterType === "LOT_SIZE");
        const notionalFilter = s.filters.find((f) => f.filterType === "NOTIONAL" || f.filterType === "MIN_NOTIONAL");
        if (lotFilter) {
          const stepSize = parseFloat(lotFilter.stepSize);
          const stepStr = Number(lotFilter.stepSize).toString();
          const precision = stepStr.includes(".") ? stepStr.split(".")[1].length : 0;
          symbolLotSizes[s.symbol] = {
            stepSize,
            precision,
            minQty: parseFloat(lotFilter.minQty),
            minNotional: parseFloat(notionalFilter?.minNotional || notionalFilter?.notional || 5),
          };
        }
      });
      console.log("Loaded exchange LOT_SIZE rules for precision formatting.");
    }
  } catch (err) {
    console.error("Failed to fetch exchange info:", err.message);
  }
}

function formatQuantity(symbol, qty) {
  const rule = symbolLotSizes[symbol.toUpperCase()];
  if (!rule) return String(qty);
  const steps = Math.floor((Number(qty) + Number.EPSILON) / rule.stepSize);
  return parseFloat((steps * rule.stepSize).toFixed(rule.precision)).toFixed(rule.precision);
}

function isDustQty(symbol, qty, price) {
  const amount = Number(qty);
  if (!Number.isFinite(amount) || amount <= 0) return true;
  const rule = symbolLotSizes[symbol.toUpperCase()];
  if (rule?.minQty && amount + Number.EPSILON < rule.minQty) return true;
  const minNotional = rule?.minNotional || 5;
  return amount * (Number(price) || 0) < minNotional;
}

async function sendTelegramAlert(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.warn("[TELEGRAM SKIPPED] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env");
    return;
  }

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: "HTML",
      }),
    });

    const data = await res.json();
    if (!res.ok || !data.ok) {
      console.error("[TELEGRAM API REJECTED]", data);
    }
  } catch (err) {
    console.error("Telegram Network Error:", err.message);
  }
}

function parseHHMM(value) {
  if (value == null || value === "") return null;
  const match = String(value).trim().match(/^([01]?\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?$/);
  if (!match) return null;
  return parseInt(match[1], 10) * 60 + parseInt(match[2], 10);
}

function normalizeAllowedTime(value) {
  if (value == null || String(value).trim() === "") return "";
  const match = String(value).trim().match(/^([01]?\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?$/);
  if (!match) return null;
  return `${String(match[1]).padStart(2, "0")}:${match[2]}`;
}

function isWithinAllowedHours(startStr, endStr, now = new Date()) {
  const start = parseHHMM(startStr);
  const end = parseHHMM(endStr);
  if (start === null || end === null) return true;
  const current = now.getHours() * 60 + now.getMinutes();
  if (start === end) return true;
  if (start < end) return current >= start && current < end;
  return current >= start || current < end;
}

function shouldSendTelegram(channel) {
  const isStocks = channel === "stocks";
  const enabled = isStocks ? alertConfig.stocksTelegramEnabled : alertConfig.cryptoTelegramEnabled;
  if (!enabled) return false;
  const start = isStocks ? alertConfig.stocksAllowedStart : alertConfig.cryptoAllowedStart;
  const end = isStocks ? alertConfig.stocksAllowedEnd : alertConfig.cryptoAllowedEnd;
  return isWithinAllowedHours(start, end);
}

function sendChannelTelegram(channel, message) {
  if (!shouldSendTelegram(channel)) return Promise.resolve();
  return sendTelegramAlert(message);
}

function parseAlertSettingsPayload(payload) {
  const values = {};

  if ("cryptoTelegramEnabled" in payload) values.cryptoTelegramEnabled = Boolean(payload.cryptoTelegramEnabled);
  if ("stocksTelegramEnabled" in payload) values.stocksTelegramEnabled = Boolean(payload.stocksTelegramEnabled);

  const next = { ...alertConfig, ...values };
  const timeKeys = ["cryptoAllowedStart", "cryptoAllowedEnd", "stocksAllowedStart", "stocksAllowedEnd"];
  for (const key of timeKeys) {
    if (key in payload) {
      const normalized = normalizeAllowedTime(payload[key]);
      if (normalized === null) return { error: "Allowed hours must be HH:MM or empty." };
      next[key] = normalized;
      values[key] = normalized;
    }
  }

  if ((next.cryptoAllowedStart && !next.cryptoAllowedEnd) || (!next.cryptoAllowedStart && next.cryptoAllowedEnd)) {
    return { error: "Set both crypto start and end times, or leave both empty for 24/7." };
  }
  if ((next.stocksAllowedStart && !next.stocksAllowedEnd) || (!next.stocksAllowedStart && next.stocksAllowedEnd)) {
    return { error: "Set both stock start and end times, or leave both empty for 24/7." };
  }

  return { values };
}

async function updateAccountBalances() {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) return;

  try {
    const timestamp = Date.now();
    const query = `timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(query).digest("hex");

    const response = await fetch(`https://api.binance.com/api/v3/account?${query}&signature=${signature}`, {
      headers: { "X-MBX-APIKEY": apiKey },
    });

    const data = await response.json();

    if (data.balances) {
      data.balances.forEach((b) => {
        const free = parseFloat(b.free);
        const locked = parseFloat(b.locked);
        if (free > 0) availableBalances[b.asset] = free;
        else delete availableBalances[b.asset];
        if (Number.isFinite(locked) && locked > 0) lockedBalances[b.asset] = locked;
        else delete lockedBalances[b.asset];
      });
    } else {
      // Print Binance rejection message directly in terminal logs
      console.error("[BINANCE ACCOUNT API ERROR]:", data);
    }
  } catch (err) {
    console.error("Failed to update background balances:", err.message);
  }

  try {
    const [usdtFunding, usdcFunding] = await Promise.all([getFundingFree("USDT"), getFundingFree("USDC")]);
    fundingFreeCache.USDT = usdtFunding;
    fundingFreeCache.USDC = usdcFunding;
  } catch (err) {
    console.warn("[STABLE FUNDING]", err.message);
  }
}

function collectSpotTradeSymbols() {
  const symbols = new Set(SYMBOLS.map((s) => s.toUpperCase()));

  Object.keys(availableBalances).forEach((asset) => {
    if (!asset || asset === "USDT" || asset === "USD") return;
    symbols.add(`${asset}USDT`);
    if (isStockAsset(asset)) {
      symbols.add(`${asset}USDT`);
      symbols.add(`${String(asset).toUpperCase().replace(/X$/, "")}USDT`);
    }
  });

  STOCK_SYMBOLS.forEach((ticker) => {
    symbols.add(`${ticker}USDT`);
    symbols.add(`${ticker}XUSDT`);
  });

  try {
    const stmt = db.prepare("SELECT DISTINCT symbol FROM trades");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.symbol) symbols.add(String(row.symbol).toUpperCase());
    }
    stmt.free();
  } catch (err) {
    console.warn("[TRADE HISTORY] Failed to read stored symbols:", err.message);
  }

  const listed = [...symbols];
  if (!Object.keys(symbolLotSizes).length) return listed;
  return listed.filter((sym) => {
    if (symbolLotSizes[sym]) return true;
    const base = String(sym).toUpperCase().replace(/USDT$|USDC$|USD$/, "");
    return isStockAsset(base);
  });
}

function knownTradeOrderIds() {
  const ids = new Set();
  try {
    const stmt = db.prepare("SELECT order_id FROM trades");
    while (stmt.step()) {
      const row = stmt.getAsObject();
      if (row.order_id) ids.add(String(row.order_id));
    }
    stmt.free();
  } catch (err) {
    console.warn("[TRADE HISTORY] Failed to read stored order ids:", err.message);
  }
  return ids;
}

async function fetchBinanceMyTrades(symbol, limit = 200) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) throw new Error("Missing API keys.");

  const timestamp = Date.now();
  const query = `symbol=${symbol}&limit=${limit}&timestamp=${timestamp}`;
  const signature = crypto.createHmac("sha256", secretKey).update(query).digest("hex");
  const response = await fetch(`https://api.binance.com/api/v3/myTrades?${query}&signature=${signature}`, {
    headers: { "X-MBX-APIKEY": apiKey },
  });
  const data = await response.json();

  if (!Array.isArray(data)) {
    if (data?.code !== -1121) {
      console.warn(`[BINANCE MYTRADES ${symbol}]`, data.msg || JSON.stringify(data));
    }
    return [];
  }
  return data;
}

function summarizeImportedFills(fills, symbol, side) {
  const baseAsset = pairBaseAsset(symbol);
  let commissionUsdt = 0;
  let baseCommission = 0;
  let qty = 0;
  let quoteQty = 0;
  let timestamp = Date.now();

  fills.forEach((fill) => {
    qty += parseFloat(fill.qty || 0);
    quoteQty += parseFloat(fill.quoteQty || 0);
    if (fill.time) timestamp = Math.min(timestamp, normalizeEpochMs(fill.time));

    const commission = parseFloat(fill.commission || 0);
    if (!commission) return;
    const asset = fill.commissionAsset;
    if (asset === "USDT" || asset === "USDC") commissionUsdt += commission;
    else if (asset === baseAsset) baseCommission += commission;
    else {
      const px = getAssetUsdPrice(asset);
      if (px) commissionUsdt += commission * px;
    }
  });

  const isBuy = side.toUpperCase() === "BUY";
  return {
    qty: isBuy ? Math.max(0, qty - baseCommission) : qty,
    usdt: isBuy ? quoteQty + commissionUsdt : Math.max(0, quoteQty - commissionUsdt),
    price: qty > 0 ? quoteQty / qty : 0,
    timestamp,
  };
}

function aggregateBinanceFills(fills) {
  const byOrder = new Map();

  fills.forEach((fill) => {
    const orderId = String(fill.orderId);
    if (!byOrder.has(orderId)) byOrder.set(orderId, []);
    byOrder.get(orderId).push(fill);
  });

  return [...byOrder.entries()].map(([orderId, orderFills]) => {
    const first = orderFills[0];
    const symbol = String(first.symbol || "").toUpperCase();
    const side = first.isBuyer ? "BUY" : "SELL";
    const economics = summarizeImportedFills(orderFills, symbol, side);

    return {
      id: `binance_${orderId}`,
      timestamp: economics.timestamp,
      symbol,
      side,
      amount: parseFloat(economics.usdt.toFixed(2)),
      status: "SUCCESS",
      outcome: `Order #${orderId}`,
      orderId: Number.isFinite(Number(orderId)) ? Number(orderId) : orderId,
      qty: economics.qty,
      price: economics.price,
      source: "binance",
    };
  });
}

function persistImportedTrades(trades) {
  const known = knownTradeOrderIds();
  let imported = 0;

  trades.forEach((trade) => {
    const orderId = trade.orderId == null ? "" : String(trade.orderId);
    if (!orderId || known.has(orderId)) return;

    db.run(`INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
      trade.symbol,
      trade.side,
      trade.price || 0,
      trade.qty || 0,
      trade.amount || 0,
      orderId,
      trade.timestamp,
      trade.source || "binance",
    ]);
    known.add(orderId);
    imported++;
  });

  if (imported) saveDatabase();
  return imported;
}

async function syncBinanceTradeHistory() {
  await updateAccountBalances();
  const symbols = collectSpotTradeSymbols();
  const fills = [];

  for (let i = 0; i < symbols.length; i += 5) {
    const batch = symbols.slice(i, i + 5);
    const results = await Promise.all(
      batch.map(async (symbol) => {
        try {
          return await fetchBinanceMyTrades(symbol);
        } catch (err) {
          console.warn(`[BINANCE MYTRADES ${symbol}]`, err.message);
          return [];
        }
      }),
    );
    results.forEach((trades) => fills.push(...trades));
  }

  const trades = aggregateBinanceFills(fills).sort((a, b) => b.timestamp - a.timestamp);
  const imported = persistImportedTrades(trades);
  return { trades, imported, symbols };
}

async function binanceSignedRequest(method, path, params = {}) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) throw new Error("Missing API keys.");

  const timestamp = Date.now();
  const search = new URLSearchParams();
  Object.entries({ ...params, timestamp, recvWindow: params.recvWindow || 60000 }).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    search.append(key, String(value));
  });
  const signature = crypto.createHmac("sha256", secretKey).update(search.toString()).digest("hex");
  search.append("signature", signature);
  const response = await fetch(`https://api.binance.com${path}?${search.toString()}`, {
    method,
    headers: { "X-MBX-APIKEY": apiKey },
  });
  const data = await response.json();
  if (data?.code && data.code !== 200 && !Array.isArray(data)) {
    console.warn(`[BINANCE ${method} ${path}]`, data.code, data.msg || JSON.stringify(data));
  }
  return data;
}

async function fetchTickerPrice(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (!sym) return null;
  const cached = tickerPriceCache[sym];
  if (cached && Date.now() - cached.at < 30000) return cached.price;
  try {
    const res = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${sym}`);
    const data = await res.json();
    const price = parseFloat(data.price);
    if (Number.isFinite(price)) {
      tickerPriceCache[sym] = { price, at: Date.now() };
      return price;
    }
  } catch (err) {
    console.warn(`[TICKER ${sym}]`, err.message);
  }
  return null;
}

function parseEquityQuotePrice(data) {
  if (!data || typeof data !== "object") return null;
  const row = Array.isArray(data) ? data[0] : data.data && typeof data.data === "object" && !Array.isArray(data.data) ? data.data : data;
  if (!row || typeof row !== "object" || (row.code && row.code !== 200 && !Array.isArray(row))) return null;
  const last = parseFloat(row.price ?? row.lastPrice ?? row.last ?? row.markPrice ?? row.close ?? row.c);
  if (Number.isFinite(last) && last > 0) return last;
  const bid = parseFloat(row.bidPrice ?? row.bid ?? row.b);
  const ask = parseFloat(row.askPrice ?? row.ask ?? row.a);
  if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0) return (bid + ask) / 2;
  if (Number.isFinite(bid) && bid > 0) return bid;
  if (Number.isFinite(ask) && ask > 0) return ask;
  return null;
}

async function fetchEquityQuote(symbol, { maxAgeMs = 30000 } = {}) {
  const ticker = stockTickerFromAsset(symbol);
  if (!ticker) return null;
  const cached = tickerPriceCache[`EQ_${ticker}`];
  if (cached && Date.now() - cached.at < maxAgeMs) return cached.price;
  try {
    const data = await binanceSignedRequest("GET", "/sapi/v1/equity/market/quote", { symbol: ticker });
    const price = parseEquityQuotePrice(data);
    if (Number.isFinite(price)) {
      tickerPriceCache[`EQ_${ticker}`] = { price, at: Date.now() };
      return price;
    }
    if (data && !data.code && !equityQuoteShapeWarned.has(ticker)) {
      equityQuoteShapeWarned.add(ticker);
      const keys = Array.isArray(data) ? `array:${data.length}` : Object.keys(data).join(",");
      console.warn(`[BINANCE EQUITY QUOTE ${ticker}] Unrecognized payload: ${keys}`);
    }
  } catch (err) {
    console.warn(`[BINANCE EQUITY QUOTE ${ticker}]`, err.message);
  }
  const live = stockMarketData[ticker]?.lastPrice;
  return Number.isFinite(live) ? live : null;
}

function getStockPrice(asset) {
  const ticker = stockTickerFromAsset(asset);
  const px = ticker ? stockMarketData[ticker]?.lastPrice : null;
  return Number.isFinite(px) && px > 0 ? px : null;
}

async function resolveUsdValue(asset, qty) {
  const amount = parseFloat(qty) || 0;
  if (!amount) return 0;
  const upper = String(asset || "").toUpperCase();
  if (upper === "USDT" || upper === "USD") return amount;
  if (isStockAsset(upper)) {
    const stockPx = getStockPrice(upper);
    if (stockPx) return amount * stockPx;
    const equityPx = await fetchEquityQuote(upper);
    if (equityPx) return amount * equityPx;
  }
  const ticker = stockTickerFromAsset(upper);
  const live =
    (await fetchTickerPrice(`${upper}USDT`)) ||
    (await fetchTickerPrice(`${ticker}USDT`)) ||
    (await fetchTickerPrice(`${ticker}XUSDT`)) ||
    (await fetchTickerPrice(`${upper}XUSDT`));
  if (live) return amount * live;
  const cached = getAssetUsdPrice(ticker) || getAssetUsdPrice(upper);
  return cached ? amount * cached : 0;
}

function normalizeEquityTrades(payload) {
  const rows = Array.isArray(payload)
    ? payload
    : Array.isArray(payload?.list)
      ? payload.list
      : Array.isArray(payload?.trades)
        ? payload.trades
        : Array.isArray(payload?.data)
          ? payload.data
          : Array.isArray(payload?.rows)
            ? payload.rows
            : [];
  return rows
    .map((row) => {
      const symbol = String(row.symbol || row.s || row.asset || "").toUpperCase();
      const orderId = row.orderId || row.order_id || row.tradeId || row.id;
      const qty = parseFloat(row.qty || row.executedQty || row.quantity || row.origQty || 0);
      const price = parseFloat(row.price || row.avgPrice || row.executedPrice || 0);
      const quote = parseFloat(row.quoteQty || row.quoteQuantity || row.executedQuoteQty || qty * price || 0);
      const sideRaw = String(row.side || row.orderSide || "").toUpperCase();
      const side = sideRaw === "BUY" || sideRaw === "SELL" ? sideRaw : row.isBuyer === false ? "SELL" : "BUY";
      const timestamp = normalizeEpochMs(row.time || row.tradeTime || row.transactTime || row.updateTime || Date.now());
      if (!symbol || !orderId) return null;
      const base = symbol.replace(/USDT$|USD$|USDC$/, "");
      if (base) binanceStockAssets.add(base);
      return {
        id: `binance_eq_${orderId}`,
        timestamp: Number.isFinite(timestamp) ? timestamp : Date.now(),
        symbol,
        side,
        amount: parseFloat(quote.toFixed(2)),
        status: "SUCCESS",
        outcome: `Binance stock #${orderId}`,
        orderId,
        qty,
        price,
        source: "binance-stock",
      };
    })
    .filter(Boolean);
}

async function fetchBinanceEquityTrades() {
  const trades = [];
  const now = Date.now();
  const week = 7 * 24 * 60 * 60 * 1000;
  for (let i = 0; i < 4; i++) {
    const endTime = now - i * week;
    const startTime = endTime - week;
    try {
      const data = await binanceSignedRequest("GET", "/sapi/v1/equity/trade/history", {
        startTime,
        endTime,
        size: 100,
      });
      if (data?.code && !Array.isArray(data) && !data.list && !data.trades && !data.rows) {
        if (i === 0) console.warn("[BINANCE EQUITY TRADES]", data.msg || JSON.stringify(data));
        break;
      }
      const batch = normalizeEquityTrades(data);
      if (i === 0 && !batch.length && data && !Array.isArray(data)) {
        console.warn("[BINANCE EQUITY TRADES] Unexpected payload keys:", Object.keys(data).join(","));
      }
      trades.push(...batch);
    } catch (err) {
      console.warn("[BINANCE EQUITY TRADES]", err.message);
      break;
    }
  }
  return trades;
}

function mapBinanceAssetRow(row, venue) {
  const asset = String(row.asset || row.coin || row.tokenizedAsset || row.symbol || "").toUpperCase();
  const free = parseFloat(row.free ?? row.available ?? row.qty ?? 0) || 0;
  const locked =
    (parseFloat(row.locked) || 0) +
    (parseFloat(row.freeze ?? row.freezeAmount) || 0) +
    (parseFloat(row.withdrawing) || 0);
  if (!asset || free + locked <= 0.0001) return null;
  if (isStockAsset(asset)) binanceStockAssets.add(asset);
  return { asset, free, locked, venue: isStockAsset(asset) ? "binance-stock" : venue };
}

function hasAssetAlias(merged, asset) {
  const ticker = stockTickerFromAsset(asset);
  const aliases = new Set([asset, ticker, `${ticker}X`, `${ticker}B`, `B${ticker}`].map((s) => String(s || "").toUpperCase()));
  for (const row of merged.values()) {
    if (aliases.has(row.asset) || aliases.has(stockTickerFromAsset(row.asset))) return true;
  }
  return false;
}

async function fetchSignedAssetRows(method, path, params, venue, label) {
  try {
    const data = await binanceSignedRequest(method, path, params);
    if (data?.code && !Array.isArray(data) && !data.list && !data.balances) return [];
    const rows = Array.isArray(data) ? data : data?.list || data?.balances || [];
    return rows.map((row) => mapBinanceAssetRow(row, venue)).filter(Boolean);
  } catch (err) {
    console.warn(`[${label}]`, err.message);
    return [];
  }
}

async function fetchBinanceFundingStocks() {
  return fetchSignedAssetRows("POST", "/sapi/v1/asset/get-funding-asset", {}, "binance-funding", "BINANCE FUNDING");
}

async function fetchBinanceUserAssets() {
  return fetchSignedAssetRows("POST", "/sapi/v3/asset/getUserAsset", { needBtcValuation: "false" }, "binance-spot", "BINANCE USER ASSETS");
}

async function fetchBinanceCapitalBalances() {
  return fetchSignedAssetRows("GET", "/sapi/v1/capital/config/getall", {}, "binance-spot", "BINANCE CAPITAL");
}

function inferHoldingsFromEquityTrades(trades) {
  const qty = new Map();
  for (const trade of trades || []) {
    const base = String(trade.symbol || "").toUpperCase().replace(/USDT$|USDC$|USD$/, "");
    if (!base) continue;
    const signed = (trade.side === "SELL" ? -1 : 1) * (parseFloat(trade.qty) || 0);
    qty.set(base, (qty.get(base) || 0) + signed);
    binanceStockAssets.add(base);
  }
  return [...qty.entries()]
    .map(([asset, free]) => (free > 0.0001 ? { asset, free, locked: 0, venue: "binance-stock" } : null))
    .filter(Boolean);
}

async function loadBinanceStockUniverse() {
  try {
    const data = await binanceSignedRequest("GET", "/sapi/v1/equity/market/tokenized-assets", {});
    const rows = Array.isArray(data) ? data : data?.list || data?.assets || data?.data || [];
    rows.forEach((row) => {
      ["asset", "tokenizedAsset", "symbol", "baseAsset"].forEach((key) => {
        const value = String(row[key] || "").toUpperCase().replace(/USDT$|USD$|USDC$/, "");
        if (value) binanceStockAssets.add(value);
      });
    });
    STOCK_SYMBOLS.forEach((ticker) => {
      stockAliases(ticker).forEach((alias) => binanceStockAssets.add(alias));
    });
    if (binanceStockAssets.size) {
      console.log(`[BINANCE STOCKS] Tracking ${binanceStockAssets.size} tokenized equity assets.`);
    }
  } catch (err) {
    console.warn("[BINANCE STOCK UNIVERSE]", err.message);
  }
}

async function collectWalletHoldings() {
  await Promise.all([updateAccountBalances(), loadBinanceStockUniverse()]);
  const [fundingAssets, userAssets, capitalAssets, equityTrades] = await Promise.all([
    fetchBinanceFundingStocks(),
    fetchBinanceUserAssets(),
    fetchBinanceCapitalBalances(),
    fetchBinanceEquityTrades(),
  ]);
  const merged = new Map();

  const addRow = (row) => {
    if (!row) return;
    const asset = String(row.asset || "").toUpperCase();
    const ticker = stockTickerFromAsset(asset);
    const venue = isStockAsset(asset) ? "binance-stock" : row.venue;
    const display = venue === "binance-stock" ? ticker || asset : asset;
    const key = venue === "binance-stock" ? `binance-stock:${display}` : `${venue}:${asset}`;
    const prev = merged.get(key);
    if (prev) {
      prev.free = Math.max(prev.free, row.free);
      prev.locked = Math.max(prev.locked, row.locked);
      return;
    }
    merged.set(key, { ...row, asset: display, venue });
  };

  Object.keys(availableBalances).forEach((asset) => {
    addRow({
      asset,
      free: availableBalances[asset] || 0,
      locked: lockedBalances[asset] || 0,
      venue: isStockAsset(asset) ? "binance-stock" : "binance-spot",
    });
  });
  userAssets.forEach(addRow);
  capitalAssets.forEach(addRow);
  fundingAssets.forEach(addRow);
  inferHoldingsFromEquityTrades(equityTrades).forEach((row) => {
    if (!hasAssetAlias(merged, row.asset)) addRow(row);
  });
  return [...merged.values()];
}

async function refreshStockPositionCache() {
  try {
    const assets = await collectWalletHoldings();
    rememberStockPositions(assets);
    const tickers = Object.keys(stockPositionCache);
    console.log(`[STOCKS] Sell watch ${tickers.length ? tickers.join(", ") : "none held"}`);
  } catch (err) {
    console.warn("[STOCK POSITIONS]", err.message);
  }
}

async function buildHoldingsList() {
  const assets = await collectWalletHoldings();
  rememberStockPositions(assets);

  const rows = [];
  for (const row of assets) {
    const total = row.free + row.locked;
    if (total <= 0.0001) continue;
    const usdValue = await resolveUsdValue(row.asset, total);
    rows.push({
      asset: row.asset,
      free: row.free.toFixed(4),
      locked: row.locked.toFixed(4),
      total: total.toFixed(4),
      usdValue: usdValue.toFixed(2),
      venue: row.venue,
    });
  }

  rows.sort((a, b) => {
    if (a.venue !== b.venue) return a.venue === "binance-stock" ? -1 : 1;
    return a.asset.localeCompare(b.asset);
  });
  const stockCount = rows.filter((row) => row.venue === "binance-stock").length;
  console.log(`[HOLDINGS] ${rows.length} assets · ${stockCount} Binance stocks`);
  return rows;
}

async function syncAllTradeHistory() {
  const [spot, equity] = await Promise.all([syncBinanceTradeHistory(), fetchBinanceEquityTrades()]);
  const equityImported = persistImportedTrades(equity);
  const trades = [...spot.trades, ...equity].sort((a, b) => b.timestamp - a.timestamp);
  return {
    trades,
    imported: spot.imported + equityImported,
    symbols: spot.symbols,
  };
}

function formatStableQty(amount) {
  const n = Number(amount);
  if (!Number.isFinite(n) || n <= 0) return "0";
  return n.toFixed(8).replace(/\.?0+$/, "");
}

async function getFundingFree(asset) {
  try {
    const upper = String(asset || "").toUpperCase();
    const data = await binanceSignedRequest("POST", "/sapi/v1/asset/get-funding-asset", { asset: upper });
    if (data?.code && !Array.isArray(data)) return 0;
    const rows = Array.isArray(data) ? data : data?.list || [];
    const row = rows.find((item) => String(item.asset || "").toUpperCase() === upper);
    if (!row) return 0;
    return parseFloat(row?.free ?? row?.available ?? 0) || 0;
  } catch (err) {
    console.warn("[BINANCE FUNDING FREE]", err.message);
    return 0;
  }
}

async function transferFundingToSpot(asset, amount) {
  const qty = formatStableQty(amount);
  if (!parseFloat(qty)) return { success: true, skipped: true };
  const data = await binanceSignedRequest("POST", "/sapi/v1/asset/transfer", {
    type: "FUNDING_MAIN",
    asset: String(asset).toUpperCase(),
    amount: qty,
  });
  if (data?.tranId || data?.txnId) {
    console.log(`[WALLET] Funding → Spot ${qty} ${asset} (tranId ${data.tranId || data.txnId})`);
    return { success: true, details: data };
  }
  return { success: false, error: data?.msg || "Funding to Spot transfer failed", details: data };
}

async function convertUsdtToUsdc(usdtAmount) {
  const qty = formatStableQty(usdtAmount);
  const data = await binanceSignedRequest("POST", "/api/v3/order", {
    symbol: "USDCUSDT",
    side: "BUY",
    type: "MARKET",
    quoteOrderQty: qty,
  });
  if (!data?.orderId) return { success: false, error: data?.msg || "USDT to USDC convert failed", details: data };
  const usdc = parseFloat(data.executedQty || 0);
  const spent = parseFloat(data.cummulativeQuoteQty || qty);
  console.log(`[WALLET] Converted ${spent} USDT → ${usdc || qty} USDC (order ${data.orderId})`);
  await updateAccountBalances();
  return { success: true, usdc, spent, details: data };
}

async function pullFundingToSpot(asset, needed) {
  const upper = String(asset || "").toUpperCase();
  const spot = availableBalances[upper] || 0;
  const shortfall = Math.max(0, needed - spot);
  if (shortfall <= 1e-8) return { success: true, skipped: true };
  const funding = await getFundingFree(upper);
  if (funding <= 1e-8) return { success: true, skipped: true, funding: 0 };
  const xfer = await transferFundingToSpot(upper, Math.min(shortfall, funding));
  if (!xfer.success) return xfer;
  await updateAccountBalances();
  return xfer;
}

async function ensureStockBuyQuote(needed) {
  const need = parseFloat(needed);
  if (!Number.isFinite(need) || need <= 0) return { error: "Enter a buy amount greater than 0." };

  await updateAccountBalances();
  const [usdcFunding, usdtFunding] = await Promise.all([getFundingFree("USDC"), getFundingFree("USDT")]);
  const spot = { USDC: availableBalances.USDC || 0, USDT: availableBalances.USDT || 0 };
  const funding = { USDC: usdcFunding, USDT: usdtFunding };
  console.log(
    `[STOCK BUY FUNDS] Spot USDC $${spot.USDC.toFixed(2)} USDT $${spot.USDT.toFixed(2)} · Funding USDC $${funding.USDC.toFixed(2)} USDT $${funding.USDT.toFixed(2)} · need $${need.toFixed(2)} USDC`,
  );

  const usdcPull = await pullFundingToSpot("USDC", need);
  if (!usdcPull.success) {
    return {
      error: `Need $${need.toFixed(2)} USDC. Moving USDC from Funding failed: ${usdcPull.error}. Enable Universal Transfer on the API key, or move USDC to Spot in the Binance app.`,
      details: usdcPull.details,
    };
  }

  let usdc = availableBalances.USDC || 0;
  if (usdc + 1e-8 >= need) return { asset: "USDC", amount: need };

  const usdcShortfall = need - usdc;
  let usdtToSpend = Math.max(usdcShortfall * 1.003, usdcShortfall + 0.02);
  if (usdtToSpend < 5) usdtToSpend = 5;

  const usdtPull = await pullFundingToSpot("USDT", usdtToSpend);
  if (!usdtPull.success) {
    return {
      error: `Need $${need.toFixed(2)} USDC. Moving USDT from Funding so it can be converted failed: ${usdtPull.error}.`,
      details: usdtPull.details,
    };
  }

  const spotUsdt = availableBalances.USDT || 0;
  if (spotUsdt + 1e-8 < usdcShortfall) {
    return {
      error: `Need $${need.toFixed(2)} USDC for this stock buy. Spot USDC $${usdc.toFixed(2)}, Spot USDT $${spotUsdt.toFixed(2)}. Binance stocks only accept USDC.`,
    };
  }

  const conv = await convertUsdtToUsdc(Math.min(spotUsdt, usdtToSpend));
  if (!conv.success) {
    return {
      error: `Binance stocks require USDC. Converting USDT failed: ${conv.error}`,
      details: conv.details,
    };
  }

  usdc = availableBalances.USDC || 0;
  if (usdc + 1e-8 < 5) {
    return { error: `Converted USDT but only have $${usdc.toFixed(2)} USDC, which is below the stock minimum.`, details: conv.details };
  }

  return { asset: "USDC", amount: Math.min(need, usdc) };
}

function isEquityInsufficientBalance(result) {
  const code = Number(result?.code);
  const msg = String(result?.msg || "").toLowerCase();
  return code === 486405 || msg.includes("insufficient balance");
}

function isEquityUnsupportedQuote(result) {
  const code = Number(result?.code);
  const msg = String(result?.msg || "").toLowerCase();
  return code === 486201 || msg.includes("quote asset is not supported");
}

async function placeEquityMarketOrder({ ticker, side, usdtAmount, quantity, sellAll = false, alertId = null, source = "manual" }) {
  const sideUpper = String(side || "").toUpperCase();
  const isSell = sideUpper === "SELL";
  const tradeAmount = usdtAmount || config.tradeAmountUsdt;
  const params = { symbol: ticker, side: sideUpper, orderType: "MARKET" };

  try {
    if (!isSell && !STOCKS_ENABLED) {
      return { success: false, error: "Stock buys are off. This desk trades Spot USDT crypto only." };
    }
    if (isSell) {
      await refreshStockPositionCache();
      const held = getHeldStockQty(ticker);
      const rawQty = sellAll || !quantity ? held : Math.min(parseFloat(quantity) || 0, held);
      if (rawQty <= 0.0000001) return { success: false, error: `No Binance ${ticker} holding to sell.` };
      params.quantity = String(rawQty);
    } else {
      const funded = await ensureStockBuyQuote(tradeAmount);
      if (funded.error) return { success: false, error: funded.error, details: funded.details };
      params.notional = Number(funded.amount).toFixed(2);
      params.quoteAsset = "USDC";
      params.walletType = "MAIN";
      console.log(`[BINANCE STOCKS] BUY ${ticker} notional=${params.notional} quoteAsset=USDC wallet=MAIN`);
    }

    const result = await binanceSignedRequest("POST", "/sapi/v1/equity/order/place", params);
    const orderId = result?.orderId || result?.order_id || result?.id;
    if (!orderId || (result?.code && result.code !== 200)) {
      const hint = isEquityUnsupportedQuote(result)
        ? " Binance stocks only accept USDC."
        : isEquityInsufficientBalance(result)
          ? " Binance stocks settle in USDC from the Spot wallet."
          : "";
      return { success: false, error: (result?.msg || "Binance stock order rejected") + hint, details: result };
    }

    const executedQty = parseFloat(result.executedQty || result.quantity || result.qty || params.quantity || 0);
    const executedPrice =
      parseFloat(result.avgPrice || result.price || result.executedPrice) || getStockPrice(ticker) || 0;
    const executedUsdt = parseFloat(result.cummulativeQuoteQty || result.notional || result.quoteQty || executedQty * executedPrice || tradeAmount || 0);
    const timestamp = Date.now();

    db.run(`INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
      ticker,
      sideUpper,
      executedPrice,
      executedQty,
      executedUsdt,
      String(orderId),
      timestamp,
      source === "bot" ? "bot" : "binance-stock",
    ]);
    saveDatabase();
    markAlertExecuted(alertId, orderId);
    await refreshStockPositionCache();

    if (source !== "bot") {
      sendTelegramAlert(
        `✅ <b>BINANCE STOCK ${sideUpper}</b>\n\n` +
          `<b>Symbol:</b> ${ticker}\n` +
          `<b>Amount:</b> $${Number(executedUsdt).toFixed(2)} (${executedQty} ${ticker})\n` +
          `<b>Price:</b> $${executedPrice}\n` +
          `<b>Order ID:</b> <code>${orderId}</code>`,
      );
    }

    return {
      success: true,
      orderId,
      symbol: ticker,
      side: sideUpper,
      executedPrice,
      executedQty,
      executedUsdt,
      details: result,
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function stableSpendable(asset) {
  const quote = String(asset || "").toUpperCase();
  const spot = availableBalances[quote] || 0;
  const funding = fundingFreeCache[quote] || 0;
  return { quote, spot, funding, total: spot + funding };
}

function stableThatCovers(amount) {
  const need = Number(amount) || 0;
  for (const quote of ["USDT", "USDC"]) {
    const spend = stableSpendable(quote);
    if (spend.total + 1e-8 >= need) return spend;
  }
  return null;
}

function tradableQuoteSymbol(symbol, quote) {
  const pair = `${pairBaseAsset(symbol)}${quote}`;
  if (quote === "USDT") return pair;
  return symbolLotSizes[pair] ? pair : null;
}

function resolveCryptoBuy(symbol, amount) {
  const need = Number(amount) || 0;
  for (const quote of ["USDT", "USDC"]) {
    const spend = stableSpendable(quote);
    if (spend.total + 1e-8 < need) continue;
    const pair = tradableQuoteSymbol(symbol, quote);
    if (!pair) continue;
    return { ...spend, symbol: pair };
  }
  return null;
}

function insufficientStableMessage(amount) {
  const need = Number(amount) || 0;
  const usdt = stableSpendable("USDT");
  const usdc = stableSpendable("USDC");
  return `Need $${need.toFixed(2)} in USDT or USDC. USDT $${usdt.total.toFixed(2)} (spot $${usdt.spot.toFixed(2)}), USDC $${usdc.total.toFixed(2)} (spot $${usdc.spot.toFixed(2)}).`;
}

async function ensureQuoteOnSpot(quote, amount) {
  await updateAccountBalances();
  const spot = availableBalances[quote] || 0;
  if (spot + 1e-8 >= amount) return { success: true, spot };
  const pulled = await pullFundingToSpot(quote, amount);
  if (!pulled.success) return pulled;
  await updateAccountBalances();
  const now = availableBalances[quote] || 0;
  if (now + 1e-8 < amount) {
    return { success: false, error: `Need $${Number(amount).toFixed(2)} ${quote} on Spot. Spot has $${now.toFixed(2)}.` };
  }
  return { success: true, spot: now };
}

async function placeMarketOrder({ symbol, side, usdtAmount, quantity, sellAll = false, alertId = null, source = "manual" }) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) return { success: false, error: "Missing API keys." };

  const symUpper = String(symbol || "").toUpperCase();
  const sideUpperEarly = String(side || "").toUpperCase();
  if (source === "bot" && sideUpperEarly === "BUY" && isPeggedSymbol(symUpper)) {
    return { success: false, error: `${symUpper} is pegged to the dollar. Autopilot will not buy it.` };
  }
  if (isBinanceStockTradeSymbol(symUpper)) {
    return placeEquityMarketOrder({
      ticker: stockTickerFromAsset(symUpper),
      side,
      usdtAmount,
      quantity,
      sellAll,
      alertId,
      source,
    });
  }
  const sideUpper = String(side || "").toUpperCase();
  const isSell = sideUpper === "SELL";
  const clearWallet = isSell && (sellAll || !quantity);
  const tradeAmount = usdtAmount || config.tradeAmountUsdt;
  let orderSymbol = symUpper;
  let quoteAsset = pairQuote(symUpper);

  if (!isSell) {
    const plan = resolveCryptoBuy(symUpper, tradeAmount);
    if (!plan) return { success: false, error: insufficientStableMessage(tradeAmount) };
    orderSymbol = plan.symbol;
    quoteAsset = plan.quote;
    const settled = await ensureQuoteOnSpot(quoteAsset, tradeAmount);
    if (!settled.success) return { success: false, error: settled.error || insufficientStableMessage(tradeAmount) };
  }

  const baseAsset = pairBaseAsset(orderSymbol);
  let queryParams = `symbol=${orderSymbol}&side=${sideUpper}&type=MARKET`;

  try {
    if (isSell) {
      await updateAccountBalances();
      const freeQty = availableBalances[baseAsset] || 0;
      const rawQty = clearWallet ? freeQty : Math.min(parseFloat(quantity) || 0, freeQty);

      if (rawQty <= 0) return { success: false, error: `No available ${baseAsset} balance to sell.` };

      const markPrice = marketData[orderSymbol]?.prices.slice(-1)[0] || marketData[symUpper]?.prices.slice(-1)[0] || 0;
      const formattedQty = formatQuantity(orderSymbol, rawQty);

      if (!parseFloat(formattedQty) || isDustQty(symUpper, formattedQty, markPrice)) {
        const dust = await convertDustToBnb(baseAsset);
        await updateAccountBalances();
        if (dust) {
          return { success: true, orderId: null, dustConverted: true, symbol: symUpper, side: "SELL", details: dust };
        }
        return { success: false, error: `${baseAsset} balance is below Binance LOT_SIZE / min notional.` };
      }

      queryParams += `&quantity=${formattedQty}`;
    } else {
      queryParams += `&quoteOrderQty=${tradeAmount}`;
    }

    const timestamp = Date.now();
    queryParams += `&timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(queryParams).digest("hex");
    const response = await fetch(`https://api.binance.com/api/v3/order?${queryParams}&signature=${signature}`, {
      method: "POST",
      headers: {
        "X-MBX-APIKEY": apiKey,
        "Content-Type": "application/x-www-form-urlencoded",
      },
    });
    const result = await response.json();

    if (!result.orderId) {
      return { success: false, error: result.msg || "Order rejected by Binance", details: result };
    }

    const economics = summarizeOrderFills(result, orderSymbol, sideUpper);
    const executedPrice = parseFloat(result.fills?.[0]?.price || marketData[orderSymbol]?.prices.slice(-1)[0] || marketData[symUpper]?.prices.slice(-1)[0] || 0);
    const executedQty = economics.netQty;
    const executedUsdt = economics.recordedUsdt || parseFloat(result.cummulativeQuoteQty || tradeAmount || executedQty * executedPrice);

    db.run(`INSERT INTO trades (symbol, side, price, qty, usdt_amount, order_id, timestamp, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
      orderSymbol,
      sideUpper,
      executedPrice,
      executedQty,
      executedUsdt,
      String(result.orderId),
      timestamp,
      source === "bot" ? "bot" : source === "binance" ? "binance" : "manual",
    ]);
    saveDatabase();
    markAlertExecuted(alertId, result.orderId);
    await updateAccountBalances();

    let dustConverted = false;
    if (clearWallet) {
      const leftover = availableBalances[baseAsset] || 0;
      if (leftover > 0 && isDustQty(orderSymbol, leftover, executedPrice)) {
        dustConverted = Boolean(await convertDustToBnb(baseAsset));
        await updateAccountBalances();
      }
    }

    if (source !== "bot") {
      sendTelegramAlert(
        `✅ <b>TRADE EXECUTED (${sideUpper})</b>\n\n` +
          `<b>Symbol:</b> ${orderSymbol}\n` +
          `<b>Amount:</b> $${executedUsdt.toFixed(2)} ${quoteAsset} (${executedQty} ${baseAsset})\n` +
          `<b>Executed Price:</b> $${executedPrice}\n` +
          (economics.commissionUsdt || economics.baseCommission
            ? `<b>Fees:</b> $${economics.commissionUsdt.toFixed(4)}` + (economics.baseCommission ? ` + ${economics.baseCommission} ${baseAsset}` : "") + `\n`
            : "") +
          (dustConverted ? `<b>Dust:</b> leftover ${baseAsset} converted to BNB\n` : "") +
          `<b>Order ID:</b> <code>${result.orderId}</code>`,
      );
    }

    return {
      success: true,
      orderId: result.orderId,
      symbol: orderSymbol,
      quoteAsset,
      side: sideUpper,
      executedPrice,
      executedQty,
      executedUsdt,
      dustConverted,
      details: result,
      economics,
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function botOpenPositionCount() {
  return Object.keys(botSession.positions || {}).length;
}

async function enforceLossFloor() {
  if (!botSession.enabled || botSession.halted || botSession.busy) return false;
  const equity = (parseFloat(botSession.cashUsdt) || 0) + markToMarketBotPositions().value;
  const floor = parseFloat(botSession.floorUsdt) || (parseFloat(botBankroll.seedUsdt) || 0) * DESK_FLOOR_FRACTION;
  if (!(floor > 0) || equity > floor + 0.05) return false;

  botSession.halted = true;
  botSession.haltReason = `Equity $${equity.toFixed(2)} hit the $${floor.toFixed(2)} floor.`;
  botSession.lastAction = botSession.haltReason;
  persistBotSession();
  console.log(`[DESK] ${botSession.haltReason}`);
  await stopBotSession();
  botSession.halted = true;
  botSession.userStopped = true;
  persistBotSession();
  sendTelegramAlert(
    `🛑 <b>DESK HALTED</b>\n\n${botSession.haltReason}\nThe desk stays off until you start it again.`,
  );
  return true;
}

function deskClip(usdt) {
  const quarter = Math.floor(usdt * DESK_FLOOR_FRACTION * 100) / 100;
  return parseFloat(Math.min(DESK_CLIP_CAP_USDT, Math.max(10, quarter)).toFixed(2));
}

async function armSpotDesk() {
  if (botSession.halted || botSession.userStopped) {
    console.log(`[DESK] left off · ${botSession.haltReason || "stopped by you"}`);
    return { armed: false, notified: false };
  }
  if (botSession.deskGeneration === DESK_GENERATION && botSession.enabled) {
    await enforceLossFloor();
    return { armed: true, notified: false };
  }

  await updateAccountBalances();
  const usdt = availableBalances.USDT || 0;
  if (usdt < 10) {
    console.log(`[DESK] Spot USDT $${usdt.toFixed(2)} is below the $10 minimum.`);
    return { armed: false, notified: false };
  }

  const allocation = parseFloat(usdt.toFixed(2));
  const clip = deskClip(allocation);
  const floor = parseFloat((allocation * DESK_FLOOR_FRACTION).toFixed(2));
  config.tradeAmountUsdt = clip;
  saveSettingToDb("tradeAmountUsdt", clip);
  alertConfig.stocksTelegramEnabled = false;
  saveSettingToDb("stocksTelegramEnabled", "false");

  botBankroll = { seedUsdt: allocation, cashUsdt: allocation };
  persistBotBankroll();
  botSession = {
    ...createIdleBotSession(),
    enabled: true,
    deskGeneration: DESK_GENERATION,
    startedAt: Date.now(),
    budgetUsdt: allocation,
    alertProfitPercent: 10,
    alertLossPercent: 25,
    startingEquity: allocation,
    cashUsdt: allocation,
    floorUsdt: floor,
    lastAction: `Armed · $${allocation.toFixed(2)} USDT · coil on majors · clip $${clip.toFixed(2)} · floor $${floor.toFixed(2)}`,
  };
  persistBotSession();
  console.log(`[DESK] ON · $${allocation.toFixed(2)} USDT · clip $${clip.toFixed(2)} · floor $${floor.toFixed(2)}`);
  sendTelegramAlert(
    `🤖 <b>SPOT DESK ON</b>\n\n` +
      `<b>Capital:</b> $${allocation.toFixed(2)} USDT\n` +
      `<b>Clip:</b> $${clip.toFixed(2)} · one position\n` +
      `<b>Floor:</b> $${floor.toFixed(2)} (stops after a 75% loss)\n` +
      `<b>Rule:</b> 8h coil break on BTC, ETH, SOL, BNB, XRP while the 1h trend is up\n` +
      `<b>Stocks:</b> off`,
  );
  void scanAutopilotUniverse().catch((err) => console.error("[DESK] scan", err.message));
  return { armed: true, notified: true };
}

async function botMaybeBuy(symbol, closePrice, rsi, coil = null) {
  if (!botSession.enabled || botSession.busy) return;
  if (await enforceLossFloor()) return;
  if (botOpenPositionCount() > 0) return;

  const amount = Math.min(config.tradeAmountUsdt, botSession.cashUsdt);
  const plan = resolveCryptoBuy(symbol, amount);
  const minNotional = getMinNotional(plan?.symbol || symbol);

  if (amount + 1e-8 < minNotional) {
    botSession.lastAction = `Skipped BUY ${symbol} · cash $${botSession.cashUsdt.toFixed(2)} below min notional`;
    persistBotSession();
    return;
  }
  if (!plan) {
    botSession.lastAction = `Skipped BUY ${symbol} · ${insufficientStableMessage(amount)}`;
    persistBotSession();
    return;
  }

  botSession.busy = true;
  try {
    const result = await placeMarketOrder({ symbol, side: "BUY", usdtAmount: amount, source: "bot" });
    if (!result.success) {
      botSession.lastAction = `BUY failed ${symbol}: ${result.error}`;
      persistBotSession();
      return;
    }

    botSession.cashUsdt = Math.max(0, botSession.cashUsdt - result.executedUsdt);
    botSession.positions[result.symbol] = {
      qty: result.executedQty,
      costUsdt: result.executedUsdt,
      entryPrice: result.executedPrice,
      orderId: result.orderId,
      openedAt: Date.now(),
      maxNetPnl: 0,
      takeProfitPct: Number.isFinite(coil?.takeProfitPct) ? coil.takeProfitPct : null,
      stopLossPct: Number.isFinite(coil?.stopLossPct) ? coil.stopLossPct : null,
      coilWidth: Number.isFinite(coil?.width) ? coil.width : null,
    };
    const coilNote = Number.isFinite(coil?.width)
      ? ` · coil ${coil.width.toFixed(2)}% · target ${coil.takeProfitPct.toFixed(2)}% · stop ${coil.stopLossPct.toFixed(2)}%`
      : "";
    botSession.lastAction = `BOUGHT ${result.symbol} $${result.executedUsdt.toFixed(2)}${coilNote}`;
    persistBotSession();
    syncBotBankroll();

    const alertId = recordStrategyAlert({ symbol: result.symbol, action: "BUY", signalType: "BOT_BUY", price: closePrice, rsi });
    markAlertExecuted(alertId, result.orderId);
    sendChannelTelegram(
      "crypto",
      `🤖 <b>AUTOPILOT BUY (${result.symbol})</b>\n\n` +
        `<b>Spent:</b> $${result.executedUsdt.toFixed(2)} ${result.quoteAsset || pairQuote(result.symbol)}\n` +
        `<b>Price:</b> $${result.executedPrice}\n` +
        `<b>RSI:</b> ${typeof rsi === "number" ? rsi.toFixed(2) : "--"}\n` +
        (Number.isFinite(coil?.width)
          ? `<b>Coil:</b> ${coil.width.toFixed(2)}% · target ${coil.takeProfitPct.toFixed(2)}% · stop ${coil.stopLossPct.toFixed(2)}%\n`
          : "") +
        `<b>Bot cash left:</b> $${botSession.cashUsdt.toFixed(2)}\n` +
        `<b>Order:</b> <code>${result.orderId}</code>`,
    );
    subscribeKline(result.symbol);
    maybeSessionPnlAlert();
  } finally {
    botSession.busy = false;
  }
}

async function botMaybeSell(symbol, reason, rsi, { force = false } = {}) {
  const pos = botSession.positions[symbol];
  if (!pos) return { success: false, error: "No autopilot position." };
  if (!force && (!botSession.enabled || botSession.busy)) return { success: false, error: "Autopilot busy." };

  const ownedBusy = !botSession.busy;
  botSession.busy = true;
  try {
    const result = await placeMarketOrder({ symbol, side: "SELL", quantity: pos.qty, sellAll: true, source: "bot" });
    if (!result.success) {
      botSession.lastAction = `SELL failed ${symbol}: ${result.error}`;
      persistBotSession();
      return result;
    }

    const proceeds = result.executedUsdt || 0;
    const pnl = proceeds - (parseFloat(pos.costUsdt) || 0);
    botSession.cashUsdt += proceeds;
    botSession.realizedPnl += pnl;
    delete botSession.positions[symbol];
    botSession.lastAction = `${reason || "SOLD"} ${symbol} $${proceeds.toFixed(2)} (${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)})`;
    persistBotSession();
    syncBotBankroll();

    const alertId = recordStrategyAlert({
      symbol,
      action: "SELL",
      signalType:
        reason === "STOP_LOSS"
          ? "BOT_STOP_LOSS"
          : reason === "TAKE_PROFIT"
            ? "BOT_TAKE_PROFIT"
            : reason === "BREAKEVEN"
              ? "BOT_BREAKEVEN"
              : reason === "TIME_STOP"
                ? "BOT_TIME_STOP"
                : reason === "FLATTEN_PEG"
                  ? "BOT_FLATTEN"
                  : "BOT_SELL",
      price: result.executedPrice,
      rsi,
    });
    markAlertExecuted(alertId, result.orderId);
    sendChannelTelegram(
      "crypto",
      `🤖 <b>AUTOPILOT ${reason || "SELL"} (${symbol})</b>\n\n` +
        `<b>Proceeds:</b> $${proceeds.toFixed(2)} USDT\n` +
        `<b>Clip P/L:</b> ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}\n` +
        `<b>Price:</b> $${result.executedPrice}\n` +
        `<b>Bot cash:</b> $${botSession.cashUsdt.toFixed(2)}\n` +
        `<b>Order:</b> <code>${result.orderId}</code>`,
    );
    maybeSessionPnlAlert();
    return result;
  } finally {
    if (ownedBusy) botSession.busy = false;
  }
}

function parseBotBudget(value) {
  const n = parseFloat(value);
  if (!Number.isFinite(n)) return { error: "Enter a USDT budget." };
  if (n < 5) return { error: "Autopilot budget must be at least $5 (Binance min notional)." };
  if (n > 200) return { error: "Autopilot budget is capped at $200 for this terminal." };
  return { value: parseFloat(n.toFixed(2)) };
}

function parseBotAlertPercent(value, label) {
  const n = parseFloat(value);
  if (!Number.isFinite(n) || n <= 0 || n > 50) return { error: `${label} must be between 0.1 and 50.` };
  return { value: parseFloat(n.toFixed(2)) };
}

async function startBotSession({ budgetUsdt, alertProfitPercent, alertLossPercent }) {
  await updateAccountBalances();
  const hasOpen = botOpenPositionCount() > 0;

  if (hasOpen) {
    botSession.enabled = true;
    botSession.halted = false;
    botSession.userStopped = false;
    botSession.alertProfitPercent = alertProfitPercent;
    botSession.alertLossPercent = alertLossPercent;
    botSession.lastAlertKind = null;
    botSession.lastAction = `Resumed · ${Object.keys(botSession.positions).join(", ")} still open`;
    persistBotSession();
    console.log(`[AUTOPILOT] RESUMED · ${Object.keys(botSession.positions).join(", ")}`);
    void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
    sendTelegramAlert(
      `🤖 <b>AUTOPILOT RESUMED</b>\n\n` +
        `<b>Open:</b> ${Object.keys(botSession.positions).join(", ")}\n` +
        `<b>Cash:</b> $${botSession.cashUsdt.toFixed(2)} USDT\n` +
        `<b>Alerts:</b> +${alertProfitPercent}% / −${alertLossPercent}%`,
    );
    return { success: true, resumed: true, status: getBotStatus() };
  }

  const parked = parseFloat(botBankroll.cashUsdt) || parseFloat(botSession.cashUsdt) || 0;
  const carry = parked >= 5;
  let allocation = budgetUsdt;
  if (carry && budgetUsdt <= parked + 0.009) {
    allocation = parseFloat(parked.toFixed(2));
  } else if (carry && budgetUsdt > parked) {
    botBankroll.seedUsdt = (parseFloat(botBankroll.seedUsdt) || parked) + (budgetUsdt - parked);
    allocation = budgetUsdt;
  }

  if (!stableThatCovers(allocation)) {
    return { success: false, error: insufficientStableMessage(allocation) };
  }

  if (!botBankroll.seedUsdt) botBankroll.seedUsdt = allocation;
  botBankroll.cashUsdt = allocation;
  persistBotBankroll();

  botSession = {
    ...createIdleBotSession(),
    enabled: true,
    startedAt: Date.now(),
    budgetUsdt: allocation,
    alertProfitPercent,
    alertLossPercent,
    startingEquity: allocation,
    cashUsdt: allocation,
    floorUsdt: parseFloat((allocation * DESK_FLOOR_FRACTION).toFixed(2)),
    halted: false,
    userStopped: false,
    deskGeneration: DESK_GENERATION,
    lastAction: carry
      ? `Armed · carrying $${allocation.toFixed(2)} USDT · waiting for an 8h coil break`
      : `Armed · $${allocation.toFixed(2)} USDT · waiting for an 8h coil break`,
  };
  persistBotSession();
  console.log(
    `[AUTOPILOT] ON · capital $${allocation.toFixed(2)}${carry ? " (carried)" : ""} · clip $${Number(config.tradeAmountUsdt).toFixed(2)} · 8h coil on ${COIL_SYMBOLS.join(", ")}`,
  );
  sendTelegramAlert(
    `🤖 <b>AUTOPILOT ON</b>\n\n` +
      `<b>Capital:</b> $${allocation.toFixed(2)} USDT${carry ? " (includes prior session P/L)" : ""}\n` +
      `<b>Clip size:</b> $${Number(config.tradeAmountUsdt).toFixed(2)} (strategy default)\n` +
      `<b>Alerts:</b> +${alertProfitPercent}% / −${alertLossPercent}% session P/L\n` +
      `<b>Pairs:</b> ${COIL_SYMBOLS.join(", ")}\n` +
      `<b>Exits:</b> target 2× the coil, stop 0.6× the coil. Flat after 12 hours if neither prints.`,
  );
  void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
  return { success: true, resumed: false, status: getBotStatus() };
}

async function stopBotSession() {
  botSession.enabled = false;
  botSession.busy = true;
  const symbols = Object.keys(botSession.positions || {});
  const errors = [];

  for (const symbol of symbols) {
    const result = await botMaybeSell(symbol, "FLATTEN", null, { force: true });
    if (result && result.success === false) errors.push(`${symbol}: ${result.error}`);
  }

  const parked = parseFloat(botSession.cashUsdt) || 0;
  const pnl = parked - (parseFloat(botSession.startingEquity) || 0);
  const seed = parseFloat(botBankroll.seedUsdt) || parseFloat(botSession.startingEquity) || parked;
  const lifetime = parked - seed;
  botBankroll.cashUsdt = parked;
  if (!botBankroll.seedUsdt) botBankroll.seedUsdt = seed;
  persistBotBankroll();
  botSession.userStopped = true;
  botSession.lastAction = botSession.halted && botSession.haltReason
    ? botSession.haltReason
    : errors.length
      ? `Stop incomplete · ${errors.join("; ")}`
      : `Stopped · parked $${parked.toFixed(2)} USDT · session ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)} · lifetime ${lifetime >= 0 ? "+" : ""}$${lifetime.toFixed(2)}`;
  persistBotSession();
  botSession.busy = false;

  sendTelegramAlert(
    `🤖 <b>AUTOPILOT OFF</b>\n\n` +
      `<b>Parked:</b> $${parked.toFixed(2)} USDT\n` +
      `<b>Session P/L:</b> ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}\n` +
      `<b>Lifetime Autopilot:</b> ${lifetime >= 0 ? "+" : ""}$${lifetime.toFixed(2)}\n` +
      (errors.length ? `<b>Unsold:</b> ${errors.join("; ")}\n` : "") +
      `<b>Note:</b> Autopilot crypto was flattened. Other Spot holdings were left alone.`,
  );

  return { success: errors.length === 0, error: errors[0] || null, status: getBotStatus() };
}

async function convertDustToBnb(asset) {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey || !asset || asset === "USDT" || asset === "BNB") return null;

  try {
    const timestamp = Date.now();
    const queryParams = `asset=${asset}&timestamp=${timestamp}`;
    const signature = crypto.createHmac("sha256", secretKey).update(queryParams).digest("hex");
    const response = await fetch(`https://api.binance.com/sapi/v1/asset/dust?${queryParams}&signature=${signature}`, {
      method: "POST",
      headers: { "X-MBX-APIKEY": apiKey },
    });
    const result = await response.json();
    if (!response.ok || result.code) {
      console.warn(`[DUST CONVERT SKIPPED ${asset}]`, result.msg || JSON.stringify(result));
      return null;
    }
    console.log(`[DUST CONVERTED] ${asset} leftover swept to BNB`);
    return result;
  } catch (err) {
    console.warn(`[DUST CONVERT ERROR ${asset}]`, err.message);
    return null;
  }
}

function applyKlineBootstrap(symbol, klines) {
  const target = ensureMarketSlot(symbol);
  const prices = klines.map((k) => parseFloat(k[4])).filter(Number.isFinite);
  const volumes = klines.map((k) => parseFloat(k[5])).filter(Number.isFinite);
  if (prices.length < 15) return target;
  target.prices = prices.slice(-100);
  target.volumes = volumes.slice(-100);
  const rsiVals = RSI.calculate({ values: target.prices, period: 14 });
  if (rsiVals.length > 0) target.lastRsi = rsiVals[rsiVals.length - 1];
  if (target.volumes.length >= 20) {
    const volSma = SMA.calculate({ values: target.volumes, period: 20 });
    const avg = volSma[volSma.length - 1];
    const lastVol = target.volumes[target.volumes.length - 1];
    target.lastVolumeSurge = Boolean(avg && lastVol > avg * config.volumeSurgeMultiplier);
  }
  return target;
}

function klineCloses(klines) {
  return (klines || []).map((k) => parseFloat(k[4])).filter(Number.isFinite);
}

function klineVolumes(klines) {
  return (klines || []).map((k) => parseFloat(k[5])).filter(Number.isFinite);
}

function rangePctFromCloses(closes) {
  if (!closes.length) return 0;
  const high = Math.max(...closes);
  const low = Math.min(...closes);
  const last = closes[closes.length - 1];
  if (!(last > 0) || high < low) return 0;
  return ((high - low) / last) * 100;
}

function dayRangePercent(ticker) {
  const high = parseFloat(ticker.highPrice);
  const low = parseFloat(ticker.lowPrice);
  const last = parseFloat(ticker.lastPrice) || (high > 0 && low > 0 ? (high + low) / 2 : 0);
  if (!(high > 0 && low > 0 && last > 0) || high < low) return 0;
  return ((high - low) / last) * 100;
}

function assessMomentumPullback(klines15, klines1h) {
  const closes15 = klineCloses(klines15);
  const vols15 = klineVolumes(klines15);
  const closes1h = klineCloses(klines1h);
  if (closes15.length < 20 || closes1h.length < 24 || vols15.length < 20) return null;
  const rsiVals = RSI.calculate({ values: closes15, period: 14 });
  if (!rsiVals || rsiVals.length < 2) return null;
  const rsi = rsiVals[rsiVals.length - 1];
  const prevRsi = rsiVals[rsiVals.length - 2];
  const smaVals = SMA.calculate({ values: closes1h, period: 24 });
  const trend = smaVals[smaVals.length - 1];
  const last = closes1h[closes1h.length - 1];
  const volSma = SMA.calculate({ values: vols15, period: 20 });
  const avgVol = volSma[volSma.length - 1];
  const lastVol = vols15[vols15.length - 1];
  if (!(trend > 0 && avgVol > 0 && last > 0)) return null;
  const uptrend = last > trend;
  const turningUp = rsi <= config.rsiOversold && rsi > prevRsi && prevRsi <= config.rsiOversold + 8;
  const surge = lastVol > avgVol * config.volumeSurgeMultiplier;
  const rangeOk = rangePctFromCloses(closes1h) >= effectiveExits().takeProfit;
  if (!(uptrend && turningUp && surge && rangeOk)) return null;
  return { rsi, prevRsi, price: last, trendStrength: last / trend - 1 };
}

async function fetchKlines(symbol, limit = 50, interval = INTERVAL) {
  const res = await fetch(
    `https://data-api.binance.vision/api/v3/klines?symbol=${String(symbol).toUpperCase()}&interval=${interval}&limit=${limit}`,
  );
  const klines = await res.json();
  return Array.isArray(klines) ? klines : [];
}

async function bootstrapHistoricalData() {
  console.log(`Bootstrapping historical candle data for: ${SYMBOLS.map((s) => s.toUpperCase()).join(", ")}...`);
  for (const symbol of SYMBOLS) {
    try {
      const klines = await fetchKlines(symbol);
      if (klines.length >= 15) applyKlineBootstrap(symbol, klines);
    } catch (err) {
      console.error(`Failed to bootstrap ${symbol}:`, err.message);
    }
  }
}

async function pickHottestUsdtSymbols(limit = BOT_UNIVERSE_SIZE) {
  const res = await fetch("https://api.binance.com/api/v3/ticker/24hr");
  const tickers = await res.json();
  if (!Array.isArray(tickers)) {
    console.warn("[AUTOPILOT] 24h ticker scan failed:", tickers?.msg || "unexpected payload");
    return [];
  }

  const minRange = effectiveExits().takeProfit;
  return tickers
    .filter((t) => isTradableUsdtSpot(t.symbol) && parseFloat(t.quoteVolume) >= BOT_MIN_QUOTE_VOLUME && dayRangePercent(t) >= minRange)
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
    .slice(0, limit)
    .map((t) => ({
      symbol: t.symbol,
      quoteVolume: parseFloat(t.quoteVolume) || 0,
      change: parseFloat(t.priceChangePercent) || 0,
    }));
}

async function flattenPeggedBotPositions() {
  if (!botSession.enabled) return;
  for (const symbol of Object.keys(botSession.positions || {})) {
    if (!isPeggedSymbol(symbol)) continue;
    console.log(`[AUTOPILOT] ${symbol} is pegged — selling back to USDT`);
    await botMaybeSell(symbol, "FLATTEN_PEG", null);
  }
}

function assessCoilBreak(klines15, klines1h, now = Date.now()) {
  const closed = (klines15 || []).filter((k) => Number(k[6]) < now);
  if (closed.length < COIL_BARS + 20) return null;
  const bar = closed[closed.length - 1];
  const prior = closed.slice(-COIL_BARS - 1, -1);
  if (prior.length < COIL_BARS) return null;

  let hi = -Infinity;
  let lo = Infinity;
  for (const k of prior) {
    hi = Math.max(hi, parseFloat(k[2]));
    lo = Math.min(lo, parseFloat(k[3]));
  }
  const close = parseFloat(bar[4]);
  if (!(close > 0) || !(hi > lo)) return null;
  const width = ((hi - lo) / close) * 100;
  if (width < COIL_MIN_WIDTH || width > COIL_MAX_WIDTH || !(close > hi)) return null;

  const vols = closed.slice(-20).map((k) => parseFloat(k[5])).filter(Number.isFinite);
  if (vols.length < 20) return null;
  const volSma = SMA.calculate({ values: vols, period: 20 });
  const avgVol = volSma[volSma.length - 1];
  const lastVol = vols[vols.length - 1];
  if (!(avgVol > 0) || !(lastVol >= avgVol * COIL_VOL_MULT)) return null;

  const hourly = (klines1h || []).filter((k) => Number(k[6]) <= Number(bar[6]));
  const closes1h = hourly.slice(-24).map((k) => parseFloat(k[4])).filter(Number.isFinite);
  if (closes1h.length < 24) return null;
  const trendSma = SMA.calculate({ values: closes1h, period: 24 });
  const trend = trendSma[trendSma.length - 1];
  const lastHour = closes1h[closes1h.length - 1];
  if (!(trend > 0 && lastHour > trend)) return null;

  const closes15 = closed.map((k) => parseFloat(k[4])).filter(Number.isFinite);
  const rsiVals = RSI.calculate({ values: closes15, period: 14 });
  const rsi = rsiVals.length ? rsiVals[rsiVals.length - 1] : null;
  return {
    price: close,
    rsi,
    width,
    volMultiple: lastVol / avgVol,
    closeTime: Number(bar[6]),
    stopLossPct: clipStopPercent(),
    takeProfitPct: clipTargetPercent(),
  };
}

let coilScanBusy = false;

async function scanAutopilotUniverse() {
  if (coilScanBusy) return;
  coilScanBusy = true;
  try {
    if (botSession.enabled && (await enforceLossFloor())) return;
    await flattenPeggedBotPositions();
    if (botSession.enabled && botOpenPositionCount() > 0) return;

    const universe = COIL_SYMBOLS.filter((symbol) => isTradableUsdtSpot(symbol) && !isPeggedSymbol(symbol));
    if (!universe.length) {
      lastSetupNote = "Flat · coil majors are not tradable yet";
      return;
    }

    const ready = [];
    for (const symbol of universe) {
      try {
        ensureMarketSlot(symbol, !isDashboardSymbol(symbol));
        const [k15, k1h] = await Promise.all([fetchKlines(symbol, 80, "15m"), fetchKlines(symbol, 48, "1h")]);
        subscribeKline(symbol);
        const setup = assessCoilBreak(k15, k1h);
        if (setup) ready.push({ symbol, ...setup });
      } catch (err) {
        console.warn(`[AUTOPILOT] coil ${symbol} failed:`, err.message);
      }
    }

    ready.sort((a, b) => b.volMultiple - a.volMultiple);
    const preview = ready.map((row) => `${row.symbol} ${row.width.toFixed(2)}%`).join(", ");
    console.log(`[AUTOPILOT] coils ${ready.length}/${universe.length} · ${preview || "none"}`);
    lastSetupNote = ready.length
      ? `Ready ${ready[0].symbol} coil ${ready[0].width.toFixed(2)}% · target ${ready[0].takeProfitPct.toFixed(2)}% · stop ${ready[0].stopLossPct.toFixed(2)}%`
      : "Flat · no 8h coil breaking out on BTC, ETH, SOL, BNB, XRP";

    const now = Date.now();
    const cooldownMs = config.cooldownMinutes * 60 * 1000;
    if (botSession.enabled && botOpenPositionCount() === 0) {
      const buy = ready.find((row) => {
        const slot = marketData[row.symbol];
        if (!slot || slot.lastCoilBar === row.closeTime) return false;
        return now - (slot.lastSignalTime || 0) > cooldownMs;
      });
      if (buy) {
        console.log(
          `[AUTOPILOT] COIL ${buy.symbol} width ${buy.width.toFixed(2)}% · target ${buy.takeProfitPct.toFixed(2)}% · stop ${buy.stopLossPct.toFixed(2)}%`,
        );
        const slot = marketData[buy.symbol];
        slot.lastCoilBar = buy.closeTime;
        slot.lastSignalTime = now;
        await botMaybeBuy(buy.symbol, buy.price, buy.rsi, {
          takeProfitPct: buy.takeProfitPct,
          stopLossPct: buy.stopLossPct,
          width: buy.width,
        });
      }
      return;
    }

    if (!botSession.enabled) {
      for (const row of ready) {
        if (!isDashboardSymbol(row.symbol)) continue;
        const plan = resolveCryptoBuy(row.symbol, config.tradeAmountUsdt);
        if (!plan) continue;
        const slot = marketData[row.symbol];
        if (!slot || slot.lastCoilBar === row.closeTime || now - slot.lastSignalTime <= cooldownMs) continue;
        slot.lastCoilBar = row.closeTime;
        slot.lastSignalTime = now;
        const usdt = stableSpendable("USDT");
        const usdc = stableSpendable("USDC");
        recordStrategyAlert({ symbol: row.symbol, action: "BUY", signalType: "BUY", price: row.price, rsi: row.rsi });
        sendChannelTelegram(
          "crypto",
          `⚡ <b>BUY SIGNAL (${row.symbol})</b>\n\n` +
            `<b>Coil:</b> ${row.width.toFixed(2)}% over the last 8 hours, just broken\n` +
            `<b>Trend:</b> 1h close is above the 24-hour average\n` +
            `<b>Target / stop:</b> ${row.takeProfitPct.toFixed(2)}% / ${row.stopLossPct.toFixed(2)}%\n` +
            `<b>Price:</b> $${row.price}\n` +
            `<b>Pay with:</b> ${plan.quote}\n` +
            `<b>USDT:</b> $${usdt.total.toFixed(2)} · <b>USDC:</b> $${usdc.total.toFixed(2)}`,
        );
      }
    }
  } catch (err) {
    console.error("[AUTOPILOT] universe scan error:", err.message);
  } finally {
    coilScanBusy = false;
  }
}

function applyStockBars(symbol, bars) {
  const target = stockMarketData[symbol];
  if (!target || !Array.isArray(bars) || bars.length === 0) return 0;

  const prices = [];
  const volumes = [];
  let lastBarTime = null;
  for (const bar of bars) {
    const close = parseFloat(bar.c);
    const volume = parseFloat(bar.v);
    if (!Number.isFinite(close)) continue;
    prices.push(close);
    if (Number.isFinite(volume)) volumes.push(volume);
    if (bar.t) lastBarTime = bar.t;
  }
  if (prices.length === 0) return 0;

  target.prices = prices.slice(-100);
  target.volumes = volumes.slice(-100);
  target.lastPrice = prices[prices.length - 1];
  target.lastBarTime = lastBarTime;

  if (target.prices.length >= 15) {
    const rsiVals = RSI.calculate({ values: target.prices, period: 14 });
    if (rsiVals.length > 0) {
      target.lastRsi = rsiVals[rsiVals.length - 1];
    }
  }
  return prices.length;
}

function quoteRangePercent(prices) {
  const window = (prices || []).slice(-20).filter((price) => Number.isFinite(price) && price > 0);
  if (window.length < 8) return null;
  const high = Math.max(...window);
  const low = Math.min(...window);
  const last = window[window.length - 1];
  if (!(last > 0)) return null;
  return ((high - low) / last) * 100;
}

function evaluateStockSignals(symbol, closePrice, volume) {
  if (!STOCKS_ENABLED) return;
  const target = stockMarketData[symbol];
  if (!target || !Number.isFinite(closePrice)) return;

  let rsi = target.lastRsi;
  if (target.prices.length >= 15) {
    const rsiValues = RSI.calculate({ values: target.prices, period: 14 });
    if (rsiValues && rsiValues.length > 0) {
      rsi = rsiValues[rsiValues.length - 1];
      target.lastRsi = rsi;
    }
  }
  if (rsi === null || rsi === undefined) return;
  const prevRsi = target.prevRsi;
  target.prevRsi = rsi;

  let isVolumeSurge = false;
  if (Number.isFinite(volume) && target.volumes.length >= 20) {
    const volSmaValues = SMA.calculate({ values: target.volumes, period: 20 });
    if (volSmaValues && volSmaValues.length > 0) {
      const avgVolume = volSmaValues[volSmaValues.length - 1];
      isVolumeSurge = volume > avgVolume * config.volumeSurgeMultiplier;
    }
  }
  target.lastVolumeSurge = isVolumeSurge;

  const now = Date.now();
  const cooldownMs = config.cooldownMinutes * 60 * 1000;
  if (now - target.lastSignalTime <= cooldownMs) return;

  const heldQty = getHeldStockQty(symbol);
  const heldUsd = heldQty * closePrice;
  const haveVolume = Number.isFinite(volume) && target.volumes.length >= 20;
  const range = quoteRangePercent(target.prices);
  // A frozen quote (weekend, halted tape) makes RSI pin at 100 and look overbought.
  if (range !== null && range < 0.02) return;
  if (!(heldUsd >= 5)) {
    const crossed = typeof prevRsi === "number" && prevRsi > config.rsiOversold && rsi <= config.rsiOversold;
    let uptrend = false;
    if (target.prices.length >= 20) {
      const smaVals = SMA.calculate({ values: target.prices, period: 20 });
      const ma = smaVals[smaVals.length - 1];
      uptrend = Boolean(ma && closePrice > ma);
    }
    if (crossed && uptrend && (!haveVolume || isVolumeSurge)) {
      target.lastSignalTime = now;
      target.lastSignal = "BUY";
      recordStrategyAlert({ symbol, action: "BUY", signalType: "STOCK_BUY", price: closePrice, rsi, source: "stocks" });
      sendChannelTelegram(
        "stocks",
        `📈 <b>STOCK PULLBACK (${symbol})</b>\n\n` +
          `<b>RSI:</b> ${rsi.toFixed(2)} just crossed ${config.rsiOversold}, and price is above its 20-minute average.\n` +
          `<b>Price:</b> $${closePrice}\n` +
          `<b>Action:</b> One clip only. Ignore this if you already hold a full position.`,
      );
    }
    return;
  }

  const sellSignal = evaluateSellSignal(symbol, closePrice, rsi);
  if (!sellSignal) {
    if (target.lastStopLossPnl !== null) {
      const netPnl = getNetPnlPercent(getAverageEntryPrice(symbol), closePrice);
      if (netPnl === null || netPnl > -effectiveExits().stopLoss) target.lastStopLossPnl = null;
    }
    return;
  }

  if (sellSignal.type === "TAKE_PROFIT") {
    target.lastSignalTime = now;
    target.lastSignal = "SELL";
    target.lastStopLossPnl = null;
    recordStrategyAlert({ symbol, action: "SELL", signalType: "STOCK_TAKE_PROFIT", price: closePrice, rsi, source: "stocks" });
    sendChannelTelegram(
      "stocks",
      `🎯 <b>STOCK TAKE PROFIT (${symbol})</b>\n\n` +
        `<b>Price:</b> $${closePrice} (Fee-adjusted entry: $${sellSignal.avgEntryPrice.toFixed(4)})\n` +
        `<b>Net PnL after fees:</b> +${sellSignal.netPnl.toFixed(2)}%\n` +
        `<b>Holding:</b> ${heldQty.toFixed(4)} · $${heldUsd.toFixed(2)}\n` +
        `<b>RSI:</b> ${rsi.toFixed(2)}\n` +
        `<b>Action:</b> SELL on the terminal to flatten on Binance.`,
    );
    return;
  }

  if (sellSignal.type === "STOP_LOSS") {
    const lastPnl = target.lastStopLossPnl;
    const isDeeperDip = lastPnl !== null && sellSignal.netPnl <= lastPnl - 1.0;
    if (lastPnl !== null && !isDeeperDip) return;
    target.lastSignalTime = now;
    target.lastSignal = "SELL";
    target.lastStopLossPnl = sellSignal.netPnl;
    recordStrategyAlert({ symbol, action: "SELL", signalType: "STOCK_STOP_LOSS", price: closePrice, rsi, source: "stocks" });
    sendChannelTelegram(
      "stocks",
      `🛑 <b>STOCK STOP LOSS (${symbol})</b>\n\n` +
        `<b>Price:</b> $${closePrice} (Fee-adjusted entry: $${sellSignal.avgEntryPrice.toFixed(4)})\n` +
        `<b>Net PnL after fees:</b> ${sellSignal.netPnl.toFixed(2)}%\n` +
        `<b>Holding:</b> ${heldQty.toFixed(4)} · $${heldUsd.toFixed(2)}\n` +
        `<b>Action:</b> SELL on the terminal to protect capital.`,
    );
    return;
  }

  if (sellSignal.type === "SELL") {
    const crossedUp = typeof prevRsi === "number" && prevRsi < config.rsiOverbought && rsi >= config.rsiOverbought && rsi < 99.5;
    if (!crossedUp) return;
    target.lastSignalTime = now;
    target.lastSignal = "SELL";
    recordStrategyAlert({ symbol, action: "SELL", signalType: "STOCK_SELL", price: closePrice, rsi, source: "stocks" });
    sendChannelTelegram(
      "stocks",
      `📉 <b>STOCK SELL (${symbol})</b>\n\n` +
        `<b>RSI:</b> ${rsi.toFixed(2)} (overbought) | <b>Price:</b> $${closePrice}\n` +
        `<b>Holding:</b> ${heldQty.toFixed(4)} · $${heldUsd.toFixed(2)}\n` +
        `<b>Action:</b> You hold this on Binance — SELL on the terminal.`,
    );
  }
}

function closeStockBar(symbol, closePrice, volume) {
  const target = stockMarketData[symbol];
  if (!target || !Number.isFinite(closePrice)) return;
  if (!target.sawLiveBar) {
    target.sawLiveBar = true;
    console.log(`[BINANCE STOCKS] first live bar ${symbol} $${closePrice}`);
  }
  target.prices.push(closePrice);
  if (Number.isFinite(volume)) target.volumes.push(volume);
  target.lastPrice = closePrice;
  if (target.prices.length > 100) target.prices.shift();
  if (target.volumes.length > 100) target.volumes.shift();
  evaluateStockSignals(symbol, closePrice, volume);
}

function ingestStockQuote(symbol, price, volume = null) {
  const target = stockMarketData[symbol];
  if (!target || !Number.isFinite(price) || price <= 0) return;
  target.lastPrice = price;
  const minuteKey = Math.floor(Date.now() / 60000);
  if (target.lastBarTime === minuteKey) {
    target.pendingClose = price;
    if (Number.isFinite(volume)) target.pendingVolume = volume;
    return;
  }
  if (target.lastBarTime != null && Number.isFinite(target.pendingClose)) {
    closeStockBar(symbol, target.pendingClose, target.pendingVolume);
  }
  target.lastBarTime = minuteKey;
  target.pendingClose = price;
  target.pendingVolume = Number.isFinite(volume) ? volume : null;
}

function normalizeKlineBars(payload) {
  const rows = Array.isArray(payload) ? payload : payload?.list || payload?.data || payload?.klines || [];
  if (!Array.isArray(rows)) return [];
  return rows
    .map((row) => {
      if (Array.isArray(row)) return { t: row[0], c: parseFloat(row[4]), v: parseFloat(row[5]) };
      return {
        t: row.t || row.openTime || row.time,
        c: parseFloat(row.c || row.close || row.price),
        v: parseFloat(row.v || row.volume),
      };
    })
    .filter((bar) => Number.isFinite(bar.c));
}

async function fetchStockKlines(ticker) {
  for (const pair of [`${ticker}USDT`, `${ticker}USDC`, `${ticker}XUSDT`]) {
    try {
      const res = await fetch(`https://api.binance.com/api/v3/klines?symbol=${pair}&interval=1m&limit=50`);
      const data = await res.json();
      const bars = normalizeKlineBars(data);
      if (bars.length) return bars;
    } catch {
      // tokenized equities are not on the public spot kline API
    }
  }
  return [];
}

async function bootstrapBinanceStocks() {
  if (STOCK_SYMBOLS.length === 0) return;
  console.log(`Bootstrapping Binance stock quotes for: ${STOCK_SYMBOLS.join(", ")}...`);
  await Promise.all(
    STOCK_SYMBOLS.map(async (symbol) => {
      const bars = await fetchStockKlines(symbol);
      if (applyStockBars(symbol, bars)) {
        stockMarketData[symbol].lastBarTime = Math.floor(Date.now() / 60000);
        stockMarketData[symbol].pendingClose = stockMarketData[symbol].lastPrice;
        console.log(`[BINANCE STOCKS] Seeded ${symbol} with ${bars.length} 1m bars, last $${stockMarketData[symbol].lastPrice}`);
        return;
      }
      const px = await fetchEquityQuote(symbol, { maxAgeMs: 0 });
      if (Number.isFinite(px)) {
        stockMarketData[symbol].lastPrice = px;
        if (!stockMarketData[symbol].prices.length) stockMarketData[symbol].prices = [px];
        console.log(`[BINANCE STOCKS] ${symbol} last $${px.toFixed(2)}`);
      } else {
        console.warn(`[BINANCE STOCKS] No quote yet for ${symbol}`);
      }
    }),
  );
}

async function pollBinanceStockQuotes() {
  const priced = [];
  for (const symbol of STOCK_SYMBOLS) {
    const px = await fetchEquityQuote(symbol, { maxAgeMs: 8000 });
    if (Number.isFinite(px)) {
      ingestStockQuote(symbol, px);
      priced.push(`${symbol} $${px.toFixed(2)}`);
    }
  }
  if (priced.length && !loggedLiveStockQuotes) {
    loggedLiveStockQuotes = true;
    console.log(`[BINANCE STOCKS] Live quotes ${priced.join(" · ")}`);
  }
}

function connectMultiStreamWS() {
  if (binanceWsReconnectTimer) {
    clearTimeout(binanceWsReconnectTimer);
    binanceWsReconnectTimer = null;
  }
  if (binanceKlineWs) {
    try {
      binanceKlineWs.removeAllListeners();
      if (binanceKlineWs.readyState === WebSocket.OPEN || binanceKlineWs.readyState === WebSocket.CONNECTING) {
        binanceKlineWs.close();
      }
    } catch {
      // ignore stale socket cleanup
    }
    binanceKlineWs = null;
  }

  const streamNames = SYMBOLS.map((s) => klineStreamName(s)).join("/");
  const wsUrl = `wss://data-stream.binance.com/stream?streams=${streamNames}`;
  const ws = new WebSocket(wsUrl);
  binanceKlineWs = ws;

  ws.on("open", () => {
    if (binanceKlineWs !== ws) return;
    SYMBOLS.forEach((s) => subscribedKlines.add(klineStreamName(s)));
    const extra = huntKlineStreams();
    extra.forEach((stream) => subscribedKlines.add(stream));
    if (extra.length) {
      ws.send(JSON.stringify({ method: "SUBSCRIBE", params: extra, id: Date.now() }));
    }
    console.log(
      `Connected to Binance Multi-Stream for: ${SYMBOLS.map((s) => s.toUpperCase()).join(", ")}` +
        (extra.length ? ` + ${extra.length} hunt pairs` : ""),
    );
  });

  ws.on("message", (data) => {
    try {
      const payload = JSON.parse(data);
      const kline = payload.data?.k;

      if (kline && kline.x) {
        const sym = kline.s;
        const closePrice = parseFloat(kline.c);
        const volume = parseFloat(kline.v);

        const target = ensureMarketSlot(sym);

        target.prices.push(closePrice);
        target.volumes.push(volume);

        if (target.prices.length > 100) target.prices.shift();
        if (target.volumes.length > 100) target.volumes.shift();

        if (target.prices.length >= 15) {
          const rsiValues = RSI.calculate({ values: target.prices, period: 14 });
          if (rsiValues && rsiValues.length > 0) {
            target.lastRsi = rsiValues[rsiValues.length - 1];
          }
        }

        let isVolumeSurge = false;
        if (target.volumes.length >= 20) {
          const volSmaValues = SMA.calculate({ values: target.volumes, period: 20 });
          if (volSmaValues && volSmaValues.length > 0) {
            const avgVolume = volSmaValues[volSmaValues.length - 1];
            isVolumeSurge = volume > avgVolume * config.volumeSurgeMultiplier;
          }
        }
        target.lastVolumeSurge = isVolumeSurge;

        const now = Date.now();
        const cooldownMs = config.cooldownMinutes * 60 * 1000;
        const cooledDown = now - target.lastSignalTime > cooldownMs;

        if (
          botSession.enabled &&
          target.lastRsi !== null &&
          (isDashboardSymbol(sym) || target.lastRsi <= config.rsiOversold + 8)
        ) {
          console.log(
            `[AUTOPILOT] ${sym} RSI ${target.lastRsi.toFixed(2)} surge=${isVolumeSurge ? "yes" : "no"} cooldown=${cooledDown ? "ready" : "blocked"} pos=${botSession.positions[sym] ? "open" : "flat"}`,
          );
        }

        if (botSession.enabled && botSession.positions[sym]) {
          const botSell = evaluateBotSellSignal(sym, closePrice, target.lastRsi, {
            fifteenMinuteClose: isFifteenMinuteClose(kline.t),
          });
          const botExits = new Set(["TAKE_PROFIT", "STOP_LOSS", "BREAKEVEN", "TIME_STOP", "FLATTEN_PEG"]);
          if (botExits.has(botSell?.type)) {
            void botMaybeSell(sym, botSell.type, target.lastRsi).catch((err) => console.error("[AUTOPILOT SELL]", err.message));
            target.lastSignalTime = now;
            target.lastStopLossPnl = botSell.type === "STOP_LOSS" ? botSell.netPnl : null;
          }
        }

        if (target.lastRsi !== null && cooledDown) {
          const baseAsset = pairBaseAsset(sym);
          const currentAssetBalance = availableBalances[baseAsset] || 0;
          const currentAssetUsdVal = currentAssetBalance * closePrice;

          if (currentAssetUsdVal >= 5.0 && !botSession.positions[sym]) {
            const sellSignal = evaluateSellSignal(sym, closePrice, target.lastRsi);

            if (sellSignal?.type === "TAKE_PROFIT") {
              recordStrategyAlert({ symbol: sym, action: "SELL", signalType: "TAKE_PROFIT", price: closePrice, rsi: target.lastRsi });
              sendChannelTelegram(
                "crypto",
                `🎯 <b>TAKE PROFIT SIGNAL (${sym})</b>\n\n` +
                  `<b>Price:</b> $${closePrice} (Fee-adjusted entry: $${sellSignal.avgEntryPrice.toFixed(4)})\n` +
                  `<b>Net PnL after fees:</b> +${sellSignal.netPnl.toFixed(2)}%\n` +
                  `<b>RSI:</b> ${target.lastRsi.toFixed(2)}`,
              );
              target.lastSignalTime = now;
              target.lastStopLossPnl = null;
            } else if (sellSignal?.type === "STOP_LOSS") {
              const lastPnl = target.lastStopLossPnl;
              const isDeeperDip = lastPnl !== null && sellSignal.netPnl <= lastPnl - 1.0;

              if (lastPnl === null || isDeeperDip) {
                recordStrategyAlert({ symbol: sym, action: "SELL", signalType: "STOP_LOSS", price: closePrice, rsi: target.lastRsi });
                sendChannelTelegram(
                  "crypto",
                  `🛑 <b>STOP LOSS ALERT (${sym})</b>\n\n` +
                    `<b>Price:</b> $${closePrice} (Fee-adjusted entry: $${sellSignal.avgEntryPrice.toFixed(4)})\n` +
                    `<b>Net PnL after fees:</b> ${sellSignal.netPnl.toFixed(2)}%\n` +
                    `<b>Action:</b> Consider selling to protect capital.`,
                );
                target.lastSignalTime = now;
                target.lastStopLossPnl = sellSignal.netPnl;
              }
            } else if (target.lastStopLossPnl !== null && (sellSignal === null || sellSignal.netPnl > -effectiveExits().stopLoss)) {
              target.lastStopLossPnl = null;
            }
          }
        }
      }
    } catch (err) {
      console.error("WS processing error:", err.message);
    }
  });

  ws.on("error", (err) => console.error("WebSocket Error:", err.message));
  ws.on("close", () => {
    if (binanceKlineWs !== ws) return;
    binanceKlineWs = null;
    if (binanceWsReconnectTimer) return;
    binanceWsReconnectTimer = setTimeout(() => {
      binanceWsReconnectTimer = null;
      connectMultiStreamWS();
    }, 3000);
  });
}

app.get("/api/settings", (req, res) => {
  res.json({
    success: true,
    config,
    alertConfig,
    botConfig,
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  });
});

app.post("/api/settings", express.json(), (req, res) => {
  const { password, settings, alertSettings, botSettings } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized password." });
  }

  const hasStrategy = settings && typeof settings === "object";
  const hasAlerts = alertSettings && typeof alertSettings === "object";
  const hasBot = botSettings && typeof botSettings === "object";
  if (!hasStrategy && !hasAlerts && !hasBot) {
    return res.status(400).json({ success: false, error: "Invalid settings payload." });
  }

  if (hasStrategy) {
    Object.keys(settings).forEach((key) => {
      if (key in config) {
        config[key] = parseFloat(settings[key]);
        saveSettingToDb(key, config[key]);
      }
    });
    console.log("Updated runtime strategy settings:", config);
  }

  if (hasAlerts) {
    const parsed = parseAlertSettingsPayload(alertSettings);
    if (parsed.error) {
      return res.status(400).json({ success: false, error: parsed.error });
    }
    Object.assign(alertConfig, parsed.values);
    Object.keys(parsed.values).forEach((key) => saveSettingToDb(key, alertConfig[key]));
    console.log("Updated runtime alert settings:", alertConfig);
  }

  if (hasBot) {
    const budget = botSettings.defaultBudgetUsdt != null ? parseBotBudget(botSettings.defaultBudgetUsdt) : null;
    const profit = botSettings.alertProfitPercent != null ? parseBotAlertPercent(botSettings.alertProfitPercent, "Profit alert %") : null;
    const loss = botSettings.alertLossPercent != null ? parseBotAlertPercent(botSettings.alertLossPercent, "Loss alert %") : null;
    if (budget?.error) return res.status(400).json({ success: false, error: budget.error });
    if (profit?.error) return res.status(400).json({ success: false, error: profit.error });
    if (loss?.error) return res.status(400).json({ success: false, error: loss.error });
    if (budget) botConfig.defaultBudgetUsdt = budget.value;
    if (profit) botConfig.alertProfitPercent = profit.value;
    if (loss) botConfig.alertLossPercent = loss.value;
    Object.keys(botConfig).forEach((key) => saveSettingToDb(key, botConfig[key]));
    console.log("Updated runtime bot settings:", botConfig);
  }

  return res.json({ success: true, config, alertConfig, botConfig });
});

app.get("/api/bot/status", (req, res) => {
  res.json({ success: true, ...getBotStatus() });
});

app.post("/api/bot/start", express.json(), async (req, res) => {
  const { password, budgetUsdt, alertProfitPercent, alertLossPercent } = req.body || {};
  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }
  if (botSession.enabled) {
    return res.status(400).json({ success: false, error: "Autopilot is already on.", status: getBotStatus() });
  }

  const budget = parseBotBudget(budgetUsdt ?? botConfig.defaultBudgetUsdt);
  const profit = parseBotAlertPercent(alertProfitPercent ?? botConfig.alertProfitPercent, "Profit alert %");
  const loss = parseBotAlertPercent(alertLossPercent ?? botConfig.alertLossPercent, "Loss alert %");
  if (budget.error) return res.status(400).json({ success: false, error: budget.error });
  if (profit.error) return res.status(400).json({ success: false, error: profit.error });
  if (loss.error) return res.status(400).json({ success: false, error: loss.error });

  botConfig.defaultBudgetUsdt = budget.value;
  botConfig.alertProfitPercent = profit.value;
  botConfig.alertLossPercent = loss.value;
  Object.keys(botConfig).forEach((key) => saveSettingToDb(key, botConfig[key]));

  const result = await startBotSession({
    budgetUsdt: budget.value,
    alertProfitPercent: profit.value,
    alertLossPercent: loss.value,
  });
  if (!result.success) return res.status(400).json(result);
  res.json(result);
});

app.post("/api/bot/stop", express.json(), async (req, res) => {
  const { password } = req.body || {};
  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }
  if (!botSession.enabled && botOpenPositionCount() === 0) {
    return res.json({ success: true, status: getBotStatus() });
  }
  const result = await stopBotSession();
  res.json(result);
});

app.post("/api/bot/alerts", express.json(), (req, res) => {
  const { password, alertProfitPercent, alertLossPercent } = req.body || {};
  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }
  const profit = parseBotAlertPercent(alertProfitPercent ?? botSession.alertProfitPercent, "Profit alert %");
  const loss = parseBotAlertPercent(alertLossPercent ?? botSession.alertLossPercent, "Loss alert %");
  if (profit.error) return res.status(400).json({ success: false, error: profit.error });
  if (loss.error) return res.status(400).json({ success: false, error: loss.error });

  botSession.alertProfitPercent = profit.value;
  botSession.alertLossPercent = loss.value;
  botSession.lastAlertKind = null;
  botConfig.alertProfitPercent = profit.value;
  botConfig.alertLossPercent = loss.value;
  Object.keys(botConfig).forEach((key) => saveSettingToDb(key, botConfig[key]));
  persistBotSession();
  res.json({ success: true, status: getBotStatus() });
});

app.get("/api/alerts", (req, res) => {
  res.json({ success: true, alerts: listStrategyAlerts() });
});

app.get("/api/stocks", (req, res) => {
  if (!STOCKS_ENABLED) {
    return res.json({
      success: true,
      enabled: false,
      source: "binance",
      feed: "off",
      connected: false,
      stocks: [],
    });
  }
  const stocks = STOCK_SYMBOLS.map((symbol) => {
    const data = stockMarketData[symbol];
    const prices = data.prices;
    const rsiVal = data.lastRsi;
    const barPrice = prices.length > 0 ? prices[prices.length - 1] : null;
    const currentPrice = Number.isFinite(data.lastPrice) ? data.lastPrice : barPrice;

    return {
      symbol,
      price: currentPrice,
      history: prices.slice(-20),
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: data.lastVolumeSurge,
      lastSignal: data.lastSignal || null,
      lastSignalTime: data.lastSignalTime || 0,
    };
  });

  res.json({
    success: true,
    source: "binance",
    feed: "equity",
    interval: "1m",
    connected: STOCK_SYMBOLS.some((symbol) => Number.isFinite(stockMarketData[symbol]?.lastPrice)),
    marketOpen: null,
    stocks,
  });
});

app.get("/api/status", (req, res) => {
  const marketsList = SYMBOLS.map((s) => s.toUpperCase()).map((sym) => {
    if (!marketData[sym]) return null;
    const prices = marketData[sym].prices;
    const rsiVal = marketData[sym].lastRsi;
    const baseAsset = pairBaseAsset(sym);
    const holdingQty = availableBalances[baseAsset] || 0;
    const currentPrice = prices.length > 0 ? prices[prices.length - 1] : 0;
    const holdingUsd = holdingQty * currentPrice;
    const avgEntryPrice = getAverageEntryPrice(sym);
    const netPnl = getNetPnlPercent(avgEntryPrice, currentPrice);
    const sellSignal = holdingUsd >= 5.0 ? evaluateSellSignal(sym, currentPrice, rsiVal) : null;

    return {
      symbol: sym,
      price: currentPrice || null,
      history: prices.slice(-20),
      rsi: typeof rsiVal === "number" ? parseFloat(rsiVal.toFixed(2)) : null,
      volSurge: marketData[sym].lastVolumeSurge,
      canBuy: Boolean(resolveCryptoBuy(sym, config.tradeAmountUsdt)),
      canSell: holdingUsd >= 5.0,
      holdingUsd: parseFloat(holdingUsd.toFixed(2)),
      avgEntryPrice: avgEntryPrice ? parseFloat(avgEntryPrice.toFixed(4)) : null,
      netPnlPercent: netPnl !== null ? parseFloat(netPnl.toFixed(2)) : null,
      sellSignal: sellSignal?.type || null,
    };
  }).filter(Boolean);

  res.json({
    interval: INTERVAL,
    markets: marketsList,
    config,
    usdtBalance: parseFloat((availableBalances.USDT || 0).toFixed(2)),
    usdcBalance: parseFloat((availableBalances.USDC || 0).toFixed(2)),
    usdtSpendable: parseFloat(stableSpendable("USDT").total.toFixed(2)),
    usdcSpendable: parseFloat(stableSpendable("USDC").total.toFixed(2)),
  });
});

app.get("/api/holdings", async (req, res) => {
  try {
    const balances = await buildHoldingsList();
    res.json({ success: true, balances });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/trades", async (req, res) => {
  const apiKey = process.env.BINANCE_API_KEY;
  const secretKey = process.env.BINANCE_SECRET_KEY;
  if (!apiKey || !secretKey) {
    return res.status(500).json({ success: false, error: "Missing API keys." });
  }

  try {
    const { trades, imported, symbols } = await syncAllTradeHistory();
    res.json({
      success: true,
      imported,
      symbols,
      trades: trades.map(({ qty, price, ...entry }) => entry),
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

function canonicalPnlSymbol(symbol) {
  const upper = String(symbol || "").toUpperCase();
  if (isBinanceStockTradeSymbol(upper)) return stockTickerFromAsset(upper);
  return upper;
}

app.get("/api/pnl", (req, res) => {
  const timeframe = req.query.tf || "all";
  let timeFilter = 0;
  const now = Date.now();

  if (timeframe === "day") timeFilter = now - 24 * 60 * 60 * 1000;
  else if (timeframe === "week") timeFilter = now - 7 * 24 * 60 * 60 * 1000;
  else if (timeframe === "month") timeFilter = now - 30 * 24 * 60 * 60 * 1000;

  try {
    // Walk the full trade history so cost basis includes buys that happened
    // before the selected window. Realized PnL is attributed to the sell time.
    const stmt = db.prepare("SELECT * FROM trades ORDER BY timestamp ASC");

    const trades = [];
    while (stmt.step()) {
      trades.push(stmt.getAsObject());
    }
    stmt.free();

    const assetPnL = {};
    let totalRealizedUsdt = 0;
    let totalVolumeTraded = 0;
    let tradeCount = 0;

    trades.forEach((t) => {
      const sym = canonicalPnlSymbol(t.symbol);
      t.timestamp = normalizeEpochMs(t.timestamp);
      if (!assetPnL[sym]) {
        assetPnL[sym] = { symbol: sym, buyQty: 0, buyCost: 0, realizedPnl: 0, tradeCount: 0 };
      }

      const inWindow = t.timestamp >= timeFilter;
      if (inWindow) {
        assetPnL[sym].tradeCount++;
        totalVolumeTraded += t.usdt_amount;
        tradeCount++;
      }

      if (t.side.toUpperCase() === "BUY") {
        assetPnL[sym].buyQty += t.qty;
        assetPnL[sym].buyCost += t.usdt_amount;
      } else if (t.side.toUpperCase() === "SELL") {
        if (assetPnL[sym].buyQty > 0) {
          const avgBuyPrice = assetPnL[sym].buyCost / assetPnL[sym].buyQty;
          const costBasisForSale = t.qty * avgBuyPrice;
          const pnl = t.usdt_amount - costBasisForSale;

          if (inWindow) {
            assetPnL[sym].realizedPnl += pnl;
            totalRealizedUsdt += pnl;
          }

          assetPnL[sym].buyQty = Math.max(0, assetPnL[sym].buyQty - t.qty);
          assetPnL[sym].buyCost = Math.max(0, assetPnL[sym].buyCost - costBasisForSale);
        }
      }
    });

    res.json({
      timeframe,
      totalRealizedPnl: parseFloat(totalRealizedUsdt.toFixed(2)),
      totalVolumeTraded: parseFloat(totalVolumeTraded.toFixed(2)),
      assets: Object.values(assetPnL)
        .filter((a) => a.tradeCount > 0)
        .map((a) => ({
          ...a,
          realizedPnl: parseFloat(a.realizedPnl.toFixed(2)),
        })),
      tradeCount,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Protected Trade Execution Endpoint
app.post("/api/trade", express.json(), async (req, res) => {
  const { symbol, side, usdtAmount, quantity, sellAll, password, alertId } = req.body;

  if (!password || password !== TRADE_PASSWORD) {
    return res.status(401).json({ success: false, error: "Unauthorized: Incorrect password." });
  }

  const result = await placeMarketOrder({ symbol, side, usdtAmount, quantity, sellAll, alertId, source: "manual" });
  if (!result.success) {
    return res.status(result.error && /Unauthorized|Missing API/.test(result.error) ? 500 : 400).json(result);
  }
  return res.json(result);
});

app.use(express.static("public"));

(async () => {
  await initDatabase();
  await fetchExchangeInfo();
  await loadBinanceStockUniverse();
  await updateAccountBalances();
  await refreshStockPositionCache();
  setInterval(updateAccountBalances, 15000);
  setInterval(() => {
    void refreshStockPositionCache().catch((err) => console.warn("[STOCK POSITIONS]", err.message));
  }, 60000);
  setInterval(() => {
    if (botSession.enabled) maybeSessionPnlAlert();
  }, 30000);
  setInterval(() => {
    void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
  }, BOT_SCAN_MS);
  await bootstrapHistoricalData();
  connectMultiStreamWS();
  if (STOCKS_ENABLED) {
    await bootstrapBinanceStocks();
    await pollBinanceStockQuotes();
    setInterval(() => {
      void pollBinanceStockQuotes().catch((err) => console.warn("[BINANCE STOCKS]", err.message));
    }, 15000);
  } else {
    console.log("[DESK] Stock quotes and stock orders are off.");
  }
  const desk = await armSpotDesk();
  setTimeout(() => {
    void scanAutopilotUniverse().catch((err) => console.error("[AUTOPILOT] scan", err.message));
  }, 8000);

  app.listen(PORT, async () => {
    console.log(`Terminal running on http://localhost:${PORT}`);
    console.log(`[AUTOPILOT] stop ${clipStopPercent()}% on a 15-minute close · target ${clipTargetPercent()}%`);
    if (desk.notified) return;
    const reason = botSession.haltReason || (botSession.userStopped ? "Desk is off." : "Desk is idle.");
    await sendTelegramAlert(`🟢 <b>Trader online</b>\n\n${reason}`);
  });
})();
