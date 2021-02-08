package com.nyble.types;

public class Subcampaign {
    private int systemId;
    private int id;
    private int brandId;

    public Subcampaign(int systemId, int id, int brandId) {
        this.systemId = systemId;
        this.id = id;
        this.brandId = brandId;
    }

    public int getSystemId() {
        return systemId;
    }

    public int getId() {
        return id;
    }

    public int getBrandId() {
        return brandId;
    }
}
