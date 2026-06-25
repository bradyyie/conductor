package com.netflix.conductor.model;

public enum WorkflowConsistency {
    // Request is kept in memory until the evaluation completes and flushed to the persistence afterwards
    // If the node dies before the writes are flushed, the workflow request is gone
    SYNCHRONOUS,

    // Default
    // Request is stored in a persistence store before the evaluation - Guarantees the execution
    // Implements durable workflows -- Suitable for most use cases
    DURABLE,

    // In the multi-region setup, guarantees that the start request is replicated across the region
    // Safest
    // Slowest
    REGION_DURABLE
}
