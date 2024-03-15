package com.github.vicrdguez;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class Stats {

    private volatile Map<String, Size> sizes;
    private static final String[] SI_UNITS = { "B", "kB", "MB", "GB", "TB", "PB", "EB" };

    public Stats() {
        sizes = new HashMap<>();
    }

    public void report() {
        Thread printThread = new Thread(new ProgressPrinter());
        printThread.start();
    }

    public void updateForTopic(String topic, int partition, int size) {
        Size s = sizes.getOrDefault(topic, new Size());
        s.add(partition, size);
        sizes.put(topic, s);
    }

    public void printResults() {
        System.out.println("====================================================================");
        System.out.print("RESULTS: \n\n");
        sizes.forEach((topicName, size) -> {
            System.out.printf("%s (total size: %s)\n",
                    Graphics.bold(topicName), humanReadableByteCount(size.topicSize()));
            partitionPrint(size.partitionSizes());
            System.out.println("");
        });
    }

    private void partitionPrint(Map<Integer, Integer> partitionSizes) {
        partitionSizes.forEach((partition, size) -> {
            // System.out.println("\tPartition " + partition + ": " + size);
            System.out.printf("Partition %d: %s\n", partition, humanReadableByteCount(size));
        });
    }

    // shamelessly stolen:
    // https://stackoverflow.com/a/33753118
    public static String humanReadableByteCount(final int bytes) {
        final String[] units = SI_UNITS;
        final int base = 1000;

        // When using the smallest unit no decimal point is needed, because it's the
        // exact number.
        if (bytes < base) {
            return bytes + " " + units[0];
        }

        final int exponent = (int) (Math.log(bytes) / Math.log(base));
        final String unit = units[exponent];
        return String.format("%.1f %s", bytes / Math.pow(base, exponent), unit);
    }

    static class Size {
        private int topicSize = 0;
        private final Map<Integer, Integer> partitionSizes = new HashMap<>();

        public void add(Integer partition, Integer size) {
            topicSize += size;
            Integer newPartitionSize = partitionSizes.getOrDefault(partition, 0) + size;
            partitionSizes.put(partition, newPartitionSize);
        }

        public int topicSize() {
            return topicSize;
        }

        public Map<Integer, Integer> partitionSizes() {
            return partitionSizes;
        }
    }

    class ProgressPrinter implements Runnable {
        private volatile boolean terminated;
        private volatile Integer lines = 0;

        @Override
        public void run() {
            // Graphics.cls();
            System.out.println("");
            while (!terminated) {
                try {
                    if (!sizes.isEmpty()) {
                        printProgress();
                    }
                } catch (Exception e) {
                    terminated = true;
                }
            }
        }

        private void printProgress() throws InterruptedException, IOException {
            // new ProcessBuilder("clear").inheritIO().start().waitFor();
            // System.out.println("lines is: " + lines);
            Graphics.gotoXY(10000000, 0);
            IntStream.range(0, 2)
                    .forEach(i -> {
                        Graphics.lineUp(1);
                        Graphics.eraseLine();
                    });
            lines = 0;
            for (Map.Entry<String, Size> e : sizes.entrySet()) {
                lines++;
                String topicSize = humanReadableByteCount(e.getValue().topicSize());
                System.out.printf("%s: %s", e.getKey(), topicSize);
                if (lines != sizes.size())
                    System.out.printf("\n");
            }
            // dirty trick
            Graphics.gotoXY(10000000, 0);
            Thread.sleep(3000);
        }

    }

}
