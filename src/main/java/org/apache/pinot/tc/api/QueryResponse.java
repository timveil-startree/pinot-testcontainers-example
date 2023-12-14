package org.apache.pinot.tc.api;

import com.google.common.base.MoreObjects;

import java.util.ArrayList;
import java.util.List;

public class QueryResponse {
    private List<QueryException> exceptions = new ArrayList<>();
    private long minConsumingFreshnessTimeMs;
    private long numConsumingSegmentsQueried;
    private long numDocsScanned;
    private long numEntriesScannedInFilter;
    private long numEntriesScannedPostFilter;
    private boolean numGroupsLimitReached;
    private long numSegmentsMatched;
    private long numSegmentsProcessed;
    private long numSegmentsQueried;
    private long numServersQueried;
    private long numServersResponded;
    private long timeUsedMs;
    private long totalDocs;

    public List<QueryException> getExceptions() {
        return exceptions;
    }

    public void setExceptions(List<QueryException> exceptions) {
        this.exceptions = exceptions;
    }

    public long getMinConsumingFreshnessTimeMs() {
        return minConsumingFreshnessTimeMs;
    }

    public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
        this.minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
    }

    public long getNumConsumingSegmentsQueried() {
        return numConsumingSegmentsQueried;
    }

    public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
        this.numConsumingSegmentsQueried = numConsumingSegmentsQueried;
    }

    public long getNumDocsScanned() {
        return numDocsScanned;
    }

    public void setNumDocsScanned(long numDocsScanned) {
        this.numDocsScanned = numDocsScanned;
    }

    public long getNumEntriesScannedInFilter() {
        return numEntriesScannedInFilter;
    }

    public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
        this.numEntriesScannedInFilter = numEntriesScannedInFilter;
    }

    public long getNumEntriesScannedPostFilter() {
        return numEntriesScannedPostFilter;
    }

    public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
        this.numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    }

    public boolean isNumGroupsLimitReached() {
        return numGroupsLimitReached;
    }

    public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
        this.numGroupsLimitReached = numGroupsLimitReached;
    }

    public long getNumSegmentsMatched() {
        return numSegmentsMatched;
    }

    public void setNumSegmentsMatched(long numSegmentsMatched) {
        this.numSegmentsMatched = numSegmentsMatched;
    }

    public long getNumSegmentsProcessed() {
        return numSegmentsProcessed;
    }

    public void setNumSegmentsProcessed(long numSegmentsProcessed) {
        this.numSegmentsProcessed = numSegmentsProcessed;
    }

    public long getNumSegmentsQueried() {
        return numSegmentsQueried;
    }

    public void setNumSegmentsQueried(long numSegmentsQueried) {
        this.numSegmentsQueried = numSegmentsQueried;
    }

    public long getNumServersQueried() {
        return numServersQueried;
    }

    public void setNumServersQueried(long numServersQueried) {
        this.numServersQueried = numServersQueried;
    }

    public long getNumServersResponded() {
        return numServersResponded;
    }

    public void setNumServersResponded(long numServersResponded) {
        this.numServersResponded = numServersResponded;
    }

    public long getTimeUsedMs() {
        return timeUsedMs;
    }

    public void setTimeUsedMs(long timeUsedMs) {
        this.timeUsedMs = timeUsedMs;
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    public void setTotalDocs(long totalDocs) {
        this.totalDocs = totalDocs;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("exceptions", exceptions)
                .add("minConsumingFreshnessTimeMs", minConsumingFreshnessTimeMs)
                .add("numConsumingSegmentsQueried", numConsumingSegmentsQueried)
                .add("numDocsScanned", numDocsScanned)
                .add("numEntriesScannedInFilter", numEntriesScannedInFilter)
                .add("numEntriesScannedPostFilter", numEntriesScannedPostFilter)
                .add("numGroupsLimitReached", numGroupsLimitReached)
                .add("numSegmentsMatched", numSegmentsMatched)
                .add("numSegmentsProcessed", numSegmentsProcessed)
                .add("numSegmentsQueried", numSegmentsQueried)
                .add("numServersQueried", numServersQueried)
                .add("numServersResponded", numServersResponded)
                .add("timeUsedMs", timeUsedMs)
                .add("totalDocs", totalDocs)
                .toString();
    }
}
