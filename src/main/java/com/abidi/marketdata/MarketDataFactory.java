package com.abidi.marketdata;

import com.abidi.marketdata.model.MarketData;
import com.abidi.util.ByteUtils;
import com.abidi.util.ChecksumUtil;

public class MarketDataFactory {

    private static final int SECURITY_LENGTH = 12;
    private static final int BROKER_LENGTH = 3;
    private static final int EXPIRES_AT_LENGTH = 19;

    private final ByteUtils byteUtils;
    private final ChecksumUtil checksumUtil;

    public MarketDataFactory() {
        this(new ByteUtils(), new ChecksumUtil());
    }

    public MarketDataFactory(ByteUtils byteUtils, ChecksumUtil checksumUtil) {
        this.byteUtils = byteUtils;
        this.checksumUtil = checksumUtil;
    }

    public MarketData create() {
        return create("AAPL00000000", 123.45, 1, true, (byte) 1, "ABC", "2026-01-01 00:00:00", 1);
    }

    public MarketData create(String secId, double price, int side, boolean isFirm,
                             byte priceType, String broker, String expiresAt, int id) {
        if (secId == null || secId.length() != SECURITY_LENGTH) {
            throw new IllegalArgumentException(
                    "secId must be exactly " + SECURITY_LENGTH + " characters but was: " + secId);
        }
        if (broker == null || broker.length() != BROKER_LENGTH) {
            throw new IllegalArgumentException(
                    "broker must be exactly " + BROKER_LENGTH + " characters but was: " + broker);
        }
        if (expiresAt == null || expiresAt.length() != EXPIRES_AT_LENGTH) {
            throw new IllegalArgumentException(
                    "expiresAt must be exactly " + EXPIRES_AT_LENGTH + " characters but was: " + expiresAt);
        }
        MarketData marketData = new MarketData(byteUtils, checksumUtil);
        marketData.set(secId, price, side, isFirm, priceType, broker, expiresAt, id);
        return marketData;
    }
}

