package com.wizjoner;

import velox.api.layer0.annotations.Layer0ReplayModule;
import velox.api.layer0.data.FileEndReachedUserMessage;
import velox.api.layer0.data.FileNotSupportedUserMessage;
import velox.api.layer0.data.ReadFileLoginData;
import velox.api.layer0.replay.ExternalReaderBaseProvider;
import velox.api.layer1.Layer1ApiListener;
import velox.api.layer1.annotations.Layer1ApiVersion;
import velox.api.layer1.annotations.Layer1ApiVersionValue;
import velox.api.layer1.data.InstrumentInfo;
import velox.api.layer1.data.TradeInfo;
import velox.api.layer1.data.LoginData;

import java.io.*;

/**
 * Nautilus CSV Replay Provider for Bookmap.
 *
 * Reads CSV files exported from Nautilus catalog and replays them
 * in Bookmap with full heatmap, orderbook depth, and trade data.
 *
 * CSV format: ts_ns,price,size,aggressor,bid,ask,bid_size,ask_size
 *
 * Install: copy JAR to ~/Library/Application Support/Bookmap/API/Layer0ApiModules/
 * Use: OPEN DATA FILE → select .mnq.csv file
 */
@Layer1ApiVersion(Layer1ApiVersionValue.VERSION2)
@Layer0ReplayModule
public class NautilusReplayProvider extends ExternalReaderBaseProvider {

    private Thread readerThread;
    private long currentTime = 0;
    private boolean play = true;
    private BufferedReader reader;
    private double pips = 0.25;

    @Override
    public void login(LoginData loginData) {
        ReadFileLoginData fileData = (ReadFileLoginData) loginData;

        try {
            String name = fileData.file.getName().toLowerCase();
            if (!name.endsWith(".mnq.csv") && !name.endsWith(".nautilus.csv")) {
                throw new IOException("Expected .mnq.csv or .nautilus.csv file");
            }

            reader = new BufferedReader(new FileReader(fileData.file));

            // Skip header
            String header = reader.readLine();
            if (header == null || !header.contains("ts_ns")) {
                throw new IOException("Invalid CSV: missing ts_ns header");
            }

            // Read first data line to get initial time + register instrument
            String firstLine = reader.readLine();
            if (firstLine == null) {
                throw new IOException("Empty CSV file");
            }
            processLine(firstLine, true);

            // Start background reader
            readerThread = new Thread(this::read, "NautilusReplay");
            readerThread.start();

        } catch (IOException e) {
            adminListeners.forEach(l ->
                l.onUserMessage(new FileNotSupportedUserMessage()));
        }
    }

    private void read() {
        try {
            String line;
            while (!Thread.interrupted() && play && (line = reader.readLine()) != null) {
                processLine(line, false);
            }
            if (play) reportFileEnd();
        } catch (IOException e) {
            reportFileEnd();
        }
    }

    private void processLine(String line, boolean isFirst) {
        try {
            String[] parts = line.split(",");
            if (parts.length < 4) return;

            long ts = Long.parseLong(parts[0].trim());
            double price = Double.parseDouble(parts[1].trim());
            int size = Integer.parseInt(parts[2].trim());
            int aggressor = Integer.parseInt(parts[3].trim());

            currentTime = ts;

            // Register instrument on first line
            if (isFirst) {
                InstrumentInfo info = new InstrumentInfo(
                    "MNQ", "MNQ", "CME", pips, 1.0, "MNQ", false, 1.0);
                instrumentListeners.forEach(l ->
                    l.onInstrumentAdded("MNQ", info));
            }

            // Send trade
            boolean isBuy = (aggressor == 1);
            TradeInfo tradeInfo = new TradeInfo(false, isBuy);
            dataListeners.forEach(l ->
                l.onTrade("MNQ", price / pips, size, tradeInfo));

            // Send depth (BBO) if available
            if (parts.length >= 8) {
                double bid = Double.parseDouble(parts[4].trim());
                double ask = Double.parseDouble(parts[5].trim());
                int bidSize = Integer.parseInt(parts[6].trim());
                int askSize = Integer.parseInt(parts[7].trim());

                if (bid > 0) {
                    int bidLevel = (int)(bid / pips);
                    dataListeners.forEach(l ->
                        l.onDepth("MNQ", true, bidLevel, bidSize));
                }
                if (ask > 0) {
                    int askLevel = (int)(ask / pips);
                    dataListeners.forEach(l ->
                        l.onDepth("MNQ", false, askLevel, askSize));
                }
            }

        } catch (NumberFormatException e) {
            // skip malformed lines
        }
    }

    private void reportFileEnd() {
        adminListeners.forEach(l ->
            l.onUserMessage(new FileEndReachedUserMessage()));
        play = false;
    }

    @Override
    public long getCurrentTime() {
        return currentTime;
    }

    @Override
    public String getSource() {
        return "Nautilus MNQ Data";
    }

    @Override
    public void close() {
        play = false;
        if (readerThread != null) readerThread.interrupt();
        try {
            if (reader != null) reader.close();
        } catch (IOException e) { }
    }
}
