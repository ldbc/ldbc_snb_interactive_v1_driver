package com.ldbc.driver;

public interface ResultReporter {
    public <OTHER_RESULT_TYPE> void report(int resultCode, OTHER_RESULT_TYPE result, Operation<OTHER_RESULT_TYPE> operation);

    public Object result();

    public int resultCode();

    public static class SimpleResultReporter implements ResultReporter {
        private Object result = null;
        private int resultCode = -1;

        public <OTHER_RESULT_TYPE> void report(int resultCode, OTHER_RESULT_TYPE result, Operation<OTHER_RESULT_TYPE> operation) {
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
