package tuxmonteiro.lab.taurina.services;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

@Service
public class ReportService {

    private static final Log LOGGER = LogFactory.getLog(ReportService.class);

    private final AtomicInteger sizeTotal = new AtomicInteger(0);
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    public void responseCount() {
        requestCounter.incrementAndGet();
    }

    public void bodySizeAccumalator(int size) {
        sizeTotal.addAndGet(size);
    }

    public void reset() {
        requestCounter.set(0);
        sizeTotal.set(0);
    }

    public void showReport(long start) {
        long durationSec = (System.currentTimeMillis() - start) / 1_000L;
        LOGGER.info("request total: " + requestCounter.get());
        LOGGER.info("size total: " + sizeTotal.get());
        LOGGER.info("rps: " + requestCounter.get() / durationSec);
        LOGGER.info("size avg: " + sizeTotal.get() / durationSec);
    }

}
