package tuxmonteiro.lab.taurina.services;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

@Service
public class ReportService {

    private static final Log LOGGER = LogFactory.getLog(ReportService.class);

    private final AtomicInteger sizeAccum = new AtomicInteger(0);
    private final AtomicInteger responseCounter = new AtomicInteger(0);
    private final AtomicInteger writeAsync = new AtomicInteger(0);
    private final AtomicInteger connCounter = new AtomicInteger(0);

    private double lastPerformanceRate = 1L;

    public void connIncr() {
        connCounter.incrementAndGet();
    }

    public void connDecr() {
        connCounter.decrementAndGet();
    }

    public void responseIncr() {
        responseCounter.incrementAndGet();
    }

    public void writeCounterIncr() {
        writeAsync.incrementAndGet();
    }

    public void bodySizeAccumalator(int size) {
        sizeAccum.addAndGet(size);
    }

    public void reset() {
        responseCounter.set(0);
        writeAsync.set(0);
        sizeAccum.set(0);
        connCounter.set(0);
    }

    public double lastPerformanceRate() {
        return lastPerformanceRate;
    }

    public void showReport(long start) {
        long durationSec = (System.currentTimeMillis() - start) / 1_000L;
        int numResp = responseCounter.get();
        int numWrites = writeAsync.get();
        int sizeTotalKb = sizeAccum.get() / 1024;
        lastPerformanceRate = (numWrites * 1.0) / (numResp * 1.0);

        LOGGER.info("total running : " + durationSec + " sec");
        LOGGER.info("conns: " + connCounter.get());
        LOGGER.info("writes total: " + numWrites);
        LOGGER.info("responses total: " + numResp);
        LOGGER.info("rate writes/resps: " + lastPerformanceRate);
        LOGGER.info("size total (kb): " + sizeTotalKb);
        LOGGER.info("rps: " + numResp / durationSec);
        LOGGER.info("size avg (kb/s): " + sizeTotalKb / durationSec);
    }

}
