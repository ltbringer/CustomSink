package spendreport;

import java.util.UUID;

public class Feature {
    private long id;
    private byte[] data;
    private long ts;

    private UUID idempotenceKey;

    public long getId() {
        return id;
    }

    public byte[] getData() {
        return data;
    }

    public long getTs() {
        return ts;
    }

    public UUID getKey() {
        return idempotenceKey;
    }

    public String toString() {
        return String.format("Feature(id=%d, ts=%s, data=%s, key=%s)", this.id, this.ts, new String(this.data), this.idempotenceKey);
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setKey(UUID idempotenceKey) {
        this.idempotenceKey = idempotenceKey;
    }
}