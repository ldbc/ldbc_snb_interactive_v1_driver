package com.ldbc.driver.runtime.executor_NEW;

// TODO add more to support current schedule technique
public enum OperationClassification {
    WindowFalse_GCTRead, // e.g., Create Post
    WindowFalse_GCTReadWrite, // no example
    WindowTrue_GCTRead, // e.g., Create Friendship
    WindowTrue_GCTReadWrite // e.g., Create User
}
