package com.ldbc.driver;

import static java.lang.String.format;

public interface ResultReporter
{
    <OTHER_RESULT_TYPE> void report( int resultCode, OTHER_RESULT_TYPE result, Operation<OTHER_RESULT_TYPE> operation );

    Object result();

    int resultCode();

    void setRunDurationAsNano( long runDurationAsNano );

    long runDurationAsNano();

    void setActualStartTimeAsMilli( long actualStartTimeAsMilli );

    long actualStartTimeAsMilli();

    class SimpleResultReporter implements ResultReporter
    {
        private Object result = null;
        private int resultCode = -1;
        private long actualStartTimeAsMilli = -1;
        private long runDurationAsNano = -1;

        public <OTHER_RESULT_TYPE> void report(
                int resultCode,
                OTHER_RESULT_TYPE result,
                Operation<OTHER_RESULT_TYPE> operation )
        {
            this.resultCode = resultCode;
            this.result = result;
            if ( null == result || null == operation )
            {
                // TODO rather thrown DbException but don't want to break existing connectors right now
                throw new RuntimeException(
                        format(
                                "Neither Operation nor Result may be null\n"
                                + "Operation: %s\n"
                                + "Result: %s",
                                operation,
                                result
                        )
                );
            }
        }

        public int resultCode()
        {
            return resultCode;
        }

        @Override
        public void setRunDurationAsNano( long runDurationAsNano )
        {
            this.runDurationAsNano = runDurationAsNano;
        }

        @Override
        public long runDurationAsNano()
        {
            return runDurationAsNano;
        }

        @Override
        public void setActualStartTimeAsMilli( long actualStartTimeAsMilli )
        {
            this.actualStartTimeAsMilli = actualStartTimeAsMilli;
        }

        @Override
        public long actualStartTimeAsMilli()
        {
            return actualStartTimeAsMilli;
        }

        public Object result()
        {
            return result;
        }
    }
}
