package org.apache.cassandra.sidecar.common.server;

/**
 * Enum for data storage units, resembles same unit names as C* is using
 */
public enum DataStorageUnit
{
    BYTES("B"),
    KIBIBYTES("KiB"),
    MEBIBYTES("MiB"),
    GIBIBYTES("GiB");

    private final String unit;

    DataStorageUnit(String unit)
    {
        this.unit = unit;
    }

    public String getValue()
    {
        return unit;
    }
}
