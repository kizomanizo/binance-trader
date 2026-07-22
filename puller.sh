#!/bin/bash
git pull &&
pm2 restart binance-trader &&
pm2 logs binance-trader --lines 100