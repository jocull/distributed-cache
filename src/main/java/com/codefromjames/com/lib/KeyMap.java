package com.codefromjames.com.lib;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyMap {
    private final Map<String, KeyData> keyMap = new HashMap<>();

    public Optional<KeyPayload> getData(String key) {
        final KeyData keyData;
        synchronized (keyMap) {
            keyData = keyMap.get(key);
        }
        if (keyData == null) {
            return Optional.empty();
        }
        return keyData.getData(key);
    }

    public void setData(KeyPayload payload) {
        final KeyData keyData;
        synchronized (keyMap) {
            keyData = keyMap.compute(payload.getKey(), (k, v) -> {
                if (v == null) {
                    return new KeyData();
                }
                return v;
            });
        }
        keyData.setData(payload);
    }
}
