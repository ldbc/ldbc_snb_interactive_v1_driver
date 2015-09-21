package com.ldbc.driver;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import static java.lang.String.format;

public interface ResultReporter
{
    <OTHER_RESULT_TYPE> void report(
            int resultCode,
            OTHER_RESULT_TYPE result,
            Operation<OTHER_RESULT_TYPE> operation ) throws DbException;

    Object result();

    int resultCode();

    long runDurationAsNano();

    long actualStartTimeAsMilli();

    class SimpleResultReporter implements ResultReporter
    {
        private final ConcurrentErrorReporter errorReporter;
        private Object result = null;
        private int resultCode = -1;
        private long actualStartTimeAsMilli = -1;
        private long runDurationAsNano = -1;

        public SimpleResultReporter( ConcurrentErrorReporter errorReporter )
        {
            this.errorReporter = errorReporter;
        }

        public <OTHER_RESULT_TYPE> void report(
                int resultCode,
                OTHER_RESULT_TYPE result,
                Operation<OTHER_RESULT_TYPE> operation ) throws DbException
        {
            this.resultCode = resultCode;
            this.result = result;
            if ( null == operation )
            {
                String errMsg = format(
                        "Operation is null\n"
                        + "Operation: %s\n"
                        + "Result: %s",
                        operation,
                        result
                );
                errorReporter.reportError( this, errMsg );
                throw new DbException( errMsg );
            }
        }

        @Override
        public int resultCode()
        {
            return resultCode;
        }

        public void setRunDurationAsNano( long runDurationAsNano )
        {
            this.runDurationAsNano = runDurationAsNano;
        }

        @Override
        public long runDurationAsNano()
        {
            return runDurationAsNano;
        }

        public void setActualStartTimeAsMilli( long actualStartTimeAsMilli )
        {
            this.actualStartTimeAsMilli = actualStartTimeAsMilli;
        }

        @Override
        public long actualStartTimeAsMilli()
        {
            return actualStartTimeAsMilli;
        }

        @Override
        public Object result()
        {
            return result;
        }

        @Override
        public String toString()
        {
            return "SimpleResultReporter{\n" +
                   "\t-->errorReporter=" + errorReporter + "\n" +
                   "\t-->result=" + result + "\n" +
                   "\t-->resultCode=" + resultCode + "\n" +
                   "\t-->actualStartTimeAsMilli=" + actualStartTimeAsMilli + "\n" +
                   "\t-->runDurationAsNano=" + runDurationAsNano + "\n" +
                   '}';
        }
    }
}
