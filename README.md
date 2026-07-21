# Binance Trader

Live 1-minute momentum and volume surge scanner for multiple trading pairs on Binance.

## Features

- **Real-time Scanner**: Monitors multiple cryptocurrency pairs with 1-minute candlestick data
- **Technical Analysis**: RSI (14) indicator with overbought/oversold detection
- **Volume Surge Detection**: Alerts when volume exceeds average thresholds
- **Live Charts**: Mini price charts for each monitored pair (last 20 candles)
- **In-App Trading**: Execute market orders directly from the dashboard
- **Toast Notifications**: Non-blocking, elegant notifications for trade events
- **Telegram Alerts**: Real-time notifications via Telegram bot
- **Responsive Design**: Dark theme UI optimized for trading terminals

## Setup

1. Clone the repository and install dependencies:
   ```bash
   npm install
   ```

2. Configure environment variables:
   ```bash
   cp example.env .env
   ```
   Then edit `.env` with your credentials:
   - Binance API keys
   - Telegram bot token and chat ID
   - Trading symbols list
   - Default trade amount

3. Start the server:
   ```bash
   node server.js
   ```

4. Open `http://localhost:3030` in your browser

## Configuration

- **SYMBOLS**: Comma-separated list of trading pairs to monitor (e.g., `btcusdt,ethusdt`)
- **DEFAULT_TRADE_AMOUNT_USDT**: Default order size in USDT
- **TRADE_PASSWORD**: Security password for trade authorization
- **INTERVAL**: Candlestick interval (1m by default)

## API Endpoints

- `GET /api/status` - Get current market data for all pairs
- `POST /api/trade` - Execute a market order

## Technology Stack

- **Backend**: Node.js, Express.js, WebSocket
- **Frontend**: Vanilla JavaScript, HTML5, CSS3
- **Charts**: Chart.js
- **Technical Analysis**: technicalindicators
- **Real-time Data**: Binance WebSocket API
