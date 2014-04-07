package com.ldbc.driver.workloads.ldbc.socnet.interactive.csv;

import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

        public void writeLine(String... columns) throws IOException {
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
        @Override
        protected OperationResult executeOperation(LdbcQuery1 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        operation.firstName());
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery2ToCsv extends OperationHandler<LdbcQuery2> {
        @Override
        protected OperationResult executeOperation(LdbcQuery2 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.maxDate().toString());
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery3ToCsv extends OperationHandler<LdbcQuery3> {
        @Override
        protected OperationResult executeOperation(LdbcQuery3 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.countryX(),
                        operation.countryY(),
                        operation.endDate().toString(),
                        Long.toString(operation.durationMillis()));
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }


    public static class LdbcQuery4ToCsv extends OperationHandler<LdbcQuery4> {
        @Override
        protected OperationResult executeOperation(LdbcQuery4 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        Long.toString(operation.minDateAsMilli()),
                        Long.toString(operation.maxDateAsMilli()),
                        Long.toString(operation.durationMillis()));
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery5ToCsv extends OperationHandler<LdbcQuery5> {
        @Override
        protected OperationResult executeOperation(LdbcQuery5 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.joinDate().toString()
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery6ToCsv extends OperationHandler<LdbcQuery6> {
        @Override
        protected OperationResult executeOperation(LdbcQuery6 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.tagName()
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery7ToCsv extends OperationHandler<LdbcQuery7> {
        @Override
        protected OperationResult executeOperation(LdbcQuery7 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId())
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery8ToCsv extends OperationHandler<LdbcQuery8> {
        @Override
        protected OperationResult executeOperation(LdbcQuery8 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId())
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery9ToCsv extends OperationHandler<LdbcQuery9> {
        @Override
        protected OperationResult executeOperation(LdbcQuery9 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        Long.toString(operation.date())
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery10ToCsv extends OperationHandler<LdbcQuery10> {
        @Override
        protected OperationResult executeOperation(LdbcQuery10 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        Integer.toString(operation.horoscopeSign1()),
                        Integer.toString(operation.horoscopeSign2())
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery11ToCsv extends OperationHandler<LdbcQuery11> {
        @Override
        protected OperationResult executeOperation(LdbcQuery11 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.country(),
                        Long.toString(operation.workFromDate())
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }

    public static class LdbcQuery12ToCsv extends OperationHandler<LdbcQuery12> {
        @Override
        protected OperationResult executeOperation(LdbcQuery12 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeLine(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.tagClass()
                );
                return null;
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e.getCause());
            }
        }
    }
}
