package com.ldbc.driver;

public interface ResultReporter {
    public <OTHER_RESULT_TYPE> void report(int resultCode, OTHER_RESULT_TYPE result, Operation<OTHER_RESULT_TYPE> operation);

    public Object result();

    public int resultCode();

    public static class SimpleResultReporter implements ResultReporter {
        private Object result = null;
        private int resultCode = -1;

        public <OTHER_RESULT_TYPE> void report(int resultCode, OTHER_RESULT_TYPE result, Operation<OTHER_RESULT_TYPE> operation) {
            if (null == result || null == operation) {
                // TODO rather thrown DbException but don't want to break existing connectors right now
                throw new RuntimeException(
                        String.format(
                                "Neither Operation nor Result may be null\n"
                                        + "Operation: %s\n"
                                        + "Result: %s",
                                operation,
                                result
                        )
                );
            }
            this.resultCode = resultCode;
            this.result = result;
        }

        public int resultCode() {
            return resultCode;
        }

        public Object result() {
            return result;
        }
    }
}
