package com.ldbc.driver;

public enum OperationClassification {
    WindowFalse_GCTRead, // e.g., Create Post
    WindowFalse_GCTReadWrite, // no example
    WindowTrue_GCTRead, // e.g., Create Friendship
    WindowTrue_GCTReadWrite // e.g., Create User
}
