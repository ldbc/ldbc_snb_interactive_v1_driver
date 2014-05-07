package com.ldbc.driver.workloads.ldbc.socnet.interactive.db;

import com.ldbc.driver.*;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvDb extends Db {
    public static final String CSV_PATH_KEY = "csv_path_key";
    private CsvDbConnectionState csvDbConnectionState;

    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        String csvPath = properties.get(CSV_PATH_KEY);
        if (null == csvPath) {
            throw new DbException(String.format("Missing parameter: %s", CSV_PATH_KEY));
        }
        try {
            File csvFile = new File(csvPath);
            csvDbConnectionState = new CsvDbConnectionState(new CsvFileWriter(csvFile));
        } catch (IOException e) {
            throw new DbException("Error encountered while trying to create CSV file writer", e.getCause());
        }
        registerOperationHandler(LdbcQuery1.class, LdbcQuery1ToCsv.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2ToCsv.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3ToCsv.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4ToCsv.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5ToCsv.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6ToCsv.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7ToCsv.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8ToCsv.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9ToCsv.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10ToCsv.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11ToCsv.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12ToCsv.class);
    }

    @Override
    protected void onCleanup() throws DbException {
        try {
            csvDbConnectionState.csvFileWriter().close();
        } catch (IOException e) {
            throw new DbException("Error encountered while trying to close CSV file writer", e.getCause());
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return csvDbConnectionState;
    }

    public class CsvDbConnectionState extends DbConnectionState {
        private final CsvFileWriter csvFileWriter;

        private CsvDbConnectionState(CsvFileWriter csvFileWriter) {
            this.csvFileWriter = csvFileWriter;
        }

        public CsvFileWriter csvFileWriter() {
            return csvFileWriter;
        }
    }

    public class CsvFileWriter {
        private final String COLUMN_SEPARATOR = ",";
        private BufferedWriter bufferedWriter = null;

        public CsvFileWriter(File file) throws IOException {
            bufferedWriter = new BufferedWriter(new FileWriter(file));
        }

        synchronized public void writeLine(String... columns) throws IOException {
            for (int i = 0; i < columns.length - 1; i++) {
                bufferedWriter.write(columns[i]);
                bufferedWriter.write(COLUMN_SEPARATOR);
            }
            bufferedWriter.write(columns[columns.length - 1]);
            bufferedWriter.newLine();
        }

        public void close() throws IOException {
            bufferedWriter.flush();
            bufferedWriter.close();
        }
    }

    public static class LdbcQuery1ToCsv extends OperationHandler<LdbcQuery1> {
        static final List<LdbcQuery1Result> RESULT = new ArrayList<LdbcQuery1Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery1 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        operation.firstName());
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery2ToCsv extends OperationHandler<LdbcQuery2> {
        static final List<LdbcQuery2Result> RESULT = new ArrayList<LdbcQuery2Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery2 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.maxDate().toString());
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery3ToCsv extends OperationHandler<LdbcQuery3> {
        static final List<LdbcQuery3Result> RESULT = new ArrayList<LdbcQuery3Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery3 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.countryX(),
                        operation.countryY(),
                        operation.endDate().toString(),
                        Long.toString(operation.durationMillis()));
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }


    public static class LdbcQuery4ToCsv extends OperationHandler<LdbcQuery4> {
        static final List<LdbcQuery4Result> RESULT = new ArrayList<LdbcQuery4Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery4 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        Long.toString(operation.minDateAsMilli()),
                        Long.toString(operation.maxDateAsMilli()),
                        Long.toString(operation.durationMillis()));
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery5ToCsv extends OperationHandler<LdbcQuery5> {
        static final List<LdbcQuery5Result> RESULT = new ArrayList<LdbcQuery5Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery5 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(Long.toString(
                        Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.joinDate().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery6ToCsv extends OperationHandler<LdbcQuery6> {
        static final List<LdbcQuery6Result> RESULT = new ArrayList<LdbcQuery6Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery6 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.tagName()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery7ToCsv extends OperationHandler<LdbcQuery7> {
        static final List<LdbcQuery7Result> RESULT = new ArrayList<LdbcQuery7Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery7 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery8ToCsv extends OperationHandler<LdbcQuery8> {
        static final List<LdbcQuery8Result> RESULT = new ArrayList<LdbcQuery8Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery8 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery9ToCsv extends OperationHandler<LdbcQuery9> {
        static final List<LdbcQuery9Result> RESULT = new ArrayList<LdbcQuery9Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery9 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        Long.toString(operation.date())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery10ToCsv extends OperationHandler<LdbcQuery10> {
        static final List<LdbcQuery10Result> RESULT = new ArrayList<LdbcQuery10Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery10 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        Integer.toString(operation.horoscopeMonth1()),
                        Integer.toString(operation.horoscopeMonth2())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery11ToCsv extends OperationHandler<LdbcQuery11> {
        static final List<LdbcQuery11Result> RESULT = new ArrayList<LdbcQuery11Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery11 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.country(),
                        Integer.toString(operation.workFromYear())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery12ToCsv extends OperationHandler<LdbcQuery12> {
        static final List<LdbcQuery12Result> RESULT = new ArrayList<LdbcQuery12Result>();

        @Override
        protected OperationResult executeOperation(LdbcQuery12 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        Long.toString(Time.nowAsMilli() - operation.scheduledStartTime().asMilli()),
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.tagClass()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }
}
