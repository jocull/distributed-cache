package com.codefromjames.com.lib.data;

import java.util.Collections;
import java.util.Map;

class DataVersion {
    private final Map<String, byte[]> data;
    private final long version;

    public DataVersion(Map<String, byte[]> data, long version) {
        this.data = Collections.unmodifiableMap(data);
        this.version = version;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public long getVersion() {
        return version;
    }
}
