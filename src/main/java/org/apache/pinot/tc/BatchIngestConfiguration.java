package org.apache.pinot.tc;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchIngestConfiguration {
    @JsonProperty("inputFormat")
    private final String inputFormat;

    @JsonProperty("recordReader.prop.delimiter")
    private final String delimiter;

    public String getInputFormat() {
        return inputFormat;
    }

    public BatchIngestConfiguration(String inputFormat, String delimiter) {
        this.inputFormat = inputFormat;
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }
}
