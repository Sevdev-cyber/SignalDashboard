package com.wizjoner;

import velox.recorder.DataRecorder;
import velox.recorder.DataRecorderConfiguration;
import velox.recorder.InstrumentDefinition;

import java.io.*;
import java.nio.file.*;

/**
 * Convert CSV tick data (pre-extracted from Nautilus parquet) → Bookmap .bmf
 *
 * Input: CSV with columns: ts_ns,price,size,aggressor,bid,ask,bid_size,ask_size
 * Output: .bmf file loadable directly in Bookmap "OPEN DATA FILE"
 *
 * Usage:
 *   java -cp "wizjoner-bridge.jar:/Applications/Bookmap.app/Contents/app/lib/*" \
 *        com.wizjoner.ParquetToBmf input.csv output.bmf
 */
public class ParquetToBmf {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: ParquetToBmf <input.csv> <output.bmf>");
            System.out.println("CSV format: ts_ns,price,size,aggressor,bid,ask,bid_size,ask_size");
            System.exit(1);
        }

        String inputCsv = args[0];
        String outputBmf = args[1];

        double pips = 0.25;
        int instrumentId = 0;

        // Create BMF recorder
        DataRecorderConfiguration config = new DataRecorderConfiguration();
        config.file = new File(outputBmf);
        config.dataSource = "Nautilus";
        DataRecorder recorder = new DataRecorder();
        try {
            recorder.init(0, config);
        } catch (Exception e) {
            System.out.println("Recorder init error (license?): " + e.getMessage());
            System.out.println("Trying without license...");
            // If license required, we can't write BMF format
            System.exit(1);
        }

        // Define instrument
        InstrumentDefinition inst = new InstrumentDefinition();
        inst.alias = "MNQ";
        inst.exchange = "CME";
        inst.type = "FUTURE";
        inst.pips = pips;
        inst.multiplier = 2.0;
        inst.depth = 10;
        inst.id = instrumentId;

        BufferedReader reader = new BufferedReader(new FileReader(inputCsv));
        String header = reader.readLine(); // skip header

        String line;
        long count = 0;
        long lastTs = 0;
        boolean instSent = false;

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            if (parts.length < 4) continue;

            long ts = Long.parseLong(parts[0]);
            double price = Double.parseDouble(parts[1]);
            int size = Integer.parseInt(parts[2]);
            int aggressor = Integer.parseInt(parts[3]);

            if (!instSent) {
                recorder.onInstrumentDefinition(ts, inst);
                instSent = true;
            }

            // Trade
            int isBidAggressor = (aggressor == 1) ? 1 : 0;
            recorder.onTrade(ts, instrumentId, price, size, isBidAggressor, 0);

            // Depth (BBO) if available
            if (parts.length >= 8) {
                double bid = Double.parseDouble(parts[4]);
                double ask = Double.parseDouble(parts[5]);
                int bidSize = Integer.parseInt(parts[6]);
                int askSize = Integer.parseInt(parts[7]);

                if (bid > 0) recorder.onDepth(ts, instrumentId, true, bid, bidSize);
                if (ask > 0) recorder.onDepth(ts, instrumentId, false, ask, askSize);
            }

            count++;
            if (count % 100000 == 0) {
                System.out.printf("  %,d ticks...%n", count);
            }
        }

        recorder.fini();
        reader.close();

        // Move the recorded file to output path
        System.out.printf("Done! %,d ticks → %s%n", count, outputBmf);
    }
}
