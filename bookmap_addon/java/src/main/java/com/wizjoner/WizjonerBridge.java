package com.wizjoner;

import velox.api.layer1.annotations.*;
import velox.api.layer1.simplified.*;
import velox.api.layer1.data.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Wizjoner ↔ Bookmap Bridge (Java addon, stable)
 *
 * Streams real-time data from Bookmap to Wizjoner signal_server via TCP:
 * - Best Bid/Ask (BBO) every tick
 * - Trades (price, size, aggressor)
 * - Depth changes (orderbook updates)
 * - Aggregated stats: bid/ask liquidity, imbalance, large trades
 *
 * Also accepts commands from Wizjoner to draw indicators on heatmap.
 *
 * Connection: TCP to localhost:9901 (or configurable)
 * Protocol: JSON lines (one JSON object per line)
 *
 * Data format sent to Wizjoner:
 *   {"type":"trade","price":24500.25,"size":3,"buy":true,"ts":1234567890}
 *   {"type":"depth","bid":true,"price":24500.0,"size":150,"ts":1234567890}
 *   {"type":"bbo","bid":24500.0,"ask":24500.25,"bidSz":45,"askSz":32,"ts":1234567890}
 *   {"type":"stats","bidLiq":12500,"askLiq":9800,"imbalance":0.12,"largeBuy":5,"largeSell":2,"ts":1234567890}
 */
@Layer1SimpleAttachable
@Layer1StrategyName("Wizjoner Bridge")
@Layer1ApiVersion(Layer1ApiVersionValue.VERSION2)
public class WizjonerBridge implements CustomModule,
        TradeDataListener, DepthDataListener, TimeListener {

    // ── Configuration ──
    private static final int TCP_PORT = 9901;
    private static final int STATS_INTERVAL_MS = 1000;  // send stats every 1s
    private static final int LARGE_TRADE_THRESHOLD = 10; // contracts
    private static final int DEPTH_LEVELS = 20;          // track 20 levels each side

    // ── State ──
    private final AtomicBoolean running = new AtomicBoolean(true);
    private PrintWriter tcpOut;
    private Socket tcpSocket;
    private ServerSocket serverSocket;
    private Thread serverThread;

    // Orderbook tracking
    private double bestBid = 0, bestAsk = 0;
    private int bestBidSize = 0, bestAskSize = 0;
    private long totalBidLiquidity = 0, totalAskLiquidity = 0;

    // Trade aggregation (reset every STATS_INTERVAL_MS)
    private int buyVolume = 0, sellVolume = 0;
    private int largeBuyCount = 0, largeSellCount = 0;
    private int tradeCount = 0;
    private long lastStatsTime = 0;

    // Instrument info
    private double pips = 0.25;
    private String alias = "";

    @Override
    public void initialize(String alias, InstrumentInfo info, Api api,
                           InitialState initialState) {
        this.alias = alias;
        this.pips = info.pips;

        System.out.println("[Wizjoner] Initialized on " + alias + " pips=" + pips);

        // Start TCP server in background
        serverThread = new Thread(this::runTcpServer, "Wizjoner-TCP");
        serverThread.setDaemon(true);
        serverThread.start();
    }

    @Override
    public void stop() {
        running.set(false);
        try {
            if (tcpSocket != null) tcpSocket.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            // ignore
        }
        System.out.println("[Wizjoner] Stopped");
    }

    // ═══════════════════════════════════════════
    // TRADE DATA
    // ═══════════════════════════════════════════

    @Override
    public void onTrade(double price, int size, TradeInfo tradeInfo) {
        boolean isBuy = tradeInfo.isBidAggressor;
        long ts = System.currentTimeMillis();

        // Aggregate
        tradeCount++;
        if (isBuy) {
            buyVolume += size;
            if (size >= LARGE_TRADE_THRESHOLD) largeBuyCount++;
        } else {
            sellVolume += size;
            if (size >= LARGE_TRADE_THRESHOLD) largeSellCount++;
        }

        // Send individual trade
        sendJson(String.format(
            "{\"type\":\"trade\",\"price\":%.2f,\"size\":%d,\"buy\":%s,\"ts\":%d}",
            price * pips, size, isBuy, ts));

        // Send stats periodically
        if (ts - lastStatsTime >= STATS_INTERVAL_MS) {
            sendStats(ts);
            lastStatsTime = ts;
        }
    }

    // ═══════════════════════════════════════════
    // DEPTH DATA (orderbook updates)
    // ═══════════════════════════════════════════

    @Override
    public void onDepth(boolean isBid, int priceLevel, int size) {
        double price = priceLevel * pips;

        // Track BBO
        if (isBid && (price > bestBid || size == 0)) {
            bestBid = price;
            bestBidSize = size;
        }
        if (!isBid && (price < bestAsk || bestAsk == 0 || size == 0)) {
            bestAsk = price;
            bestAskSize = size;
        }

        // Send depth update (only significant changes to reduce bandwidth)
        if (size >= 5 || size == 0) {
            sendJson(String.format(
                "{\"type\":\"depth\",\"bid\":%s,\"price\":%.2f,\"size\":%d}",
                isBid, price, size));
        }
    }

    // ═══════════════════════════════════════════
    // TIME UPDATES
    // ═══════════════════════════════════════════

    @Override
    public void onTimestamp(long nanoseconds) {
        // Could be used for time sync if needed
    }

    // ═══════════════════════════════════════════
    // STATS AGGREGATION
    // ═══════════════════════════════════════════

    private void sendStats(long ts) {
        int totalVol = buyVolume + sellVolume;
        double imbalance = totalVol > 0 ? (double)(buyVolume - sellVolume) / totalVol : 0;
        int delta = buyVolume - sellVolume;

        sendJson(String.format(
            "{\"type\":\"stats\",\"delta\":%d,\"buyVol\":%d,\"sellVol\":%d," +
            "\"imbalance\":%.3f,\"largeBuy\":%d,\"largeSell\":%d," +
            "\"trades\":%d,\"bid\":%.2f,\"ask\":%.2f,\"bidSz\":%d,\"askSz\":%d,\"ts\":%d}",
            delta, buyVolume, sellVolume, imbalance,
            largeBuyCount, largeSellCount, tradeCount,
            bestBid, bestAsk, bestBidSize, bestAskSize, ts));

        // Reset counters
        buyVolume = 0;
        sellVolume = 0;
        largeBuyCount = 0;
        largeSellCount = 0;
        tradeCount = 0;
    }

    // ═══════════════════════════════════════════
    // TCP SERVER (Wizjoner connects here)
    // ═══════════════════════════════════════════

    private void runTcpServer() {
        while (running.get()) {
            try {
                serverSocket = new ServerSocket(TCP_PORT);
                serverSocket.setReuseAddress(true);
                System.out.println("[Wizjoner] TCP server listening on port " + TCP_PORT);

                while (running.get()) {
                    tcpSocket = serverSocket.accept();
                    tcpOut = new PrintWriter(
                        new BufferedWriter(new OutputStreamWriter(tcpSocket.getOutputStream())),
                        true);
                    System.out.println("[Wizjoner] Client connected: " +
                        tcpSocket.getRemoteSocketAddress());

                    // Read commands from Wizjoner (optional, for future use)
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(tcpSocket.getInputStream()));
                    try {
                        String line;
                        while (running.get() && (line = in.readLine()) != null) {
                            handleCommand(line);
                        }
                    } catch (IOException e) {
                        System.out.println("[Wizjoner] Client disconnected");
                    }
                    tcpOut = null;
                }
            } catch (IOException e) {
                if (running.get()) {
                    System.out.println("[Wizjoner] TCP error: " + e.getMessage());
                    try { Thread.sleep(3000); } catch (InterruptedException ie) { break; }
                }
            }
        }
    }

    private void sendJson(String json) {
        PrintWriter out = tcpOut;
        if (out != null) {
            try {
                out.println(json);
            } catch (Exception e) {
                // Connection lost, will be re-established
                tcpOut = null;
            }
        }
    }

    private void handleCommand(String line) {
        // Future: accept commands from Wizjoner
        // e.g. draw indicators, place orders
        System.out.println("[Wizjoner] CMD: " + line);
    }
}
