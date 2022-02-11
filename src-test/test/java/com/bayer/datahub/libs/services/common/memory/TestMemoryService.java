package com.bayer.datahub.libs.services.common.memory;

public class TestMemoryService extends MemoryServiceImpl {
    private Long totalMemory;
    private Long maxMemory;
    private Long freeMemory;
    private Long usedMemoryHistoricalMax;

    public TestMemoryService() {
    }

    public TestMemoryService(Long totalMemory, Long maxMemory, Long freeMemory, Long usedMemoryHistoricalMax) {
        this.totalMemory = totalMemory;
        this.maxMemory = maxMemory;
        this.freeMemory = freeMemory;
        this.usedMemoryHistoricalMax = usedMemoryHistoricalMax;
    }

    @Override
    public Long getTotalMemoryBytes() {
        return totalMemory;
    }

    @Override
    public Long getFreeMemoryBytes() {
        return freeMemory;
    }

    @Override
    public Long getMaxMemoryBytes() {
        return maxMemory;
    }

    @Override
    public Long getUsedMemoryBytes() {
        return getTotalMemoryBytes() - getFreeMemoryBytes();
    }

    @Override
    public Long getUsedMemoryHistoricalMaxBytes() {
        return usedMemoryHistoricalMax;
    }

    public void setTotalMemory(Long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public void setMaxMemory(Long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public void setFreeMemory(Long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public void setUsedMemoryHistoricalMax(Long usedMemoryHistoricalMax) {
        this.usedMemoryHistoricalMax = usedMemoryHistoricalMax;
    }
}
