package com.ldbc.driver.workloads.ldbc.socnet.interactive.db;

import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;

import java.util.Map;

public class NothingDb extends Db {
    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
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
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return null;
    }

    public static class LdbcQuery1ToCsv extends OperationHandler<LdbcQuery1> {
        @Override
        protected OperationResult executeOperation(LdbcQuery1 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery2ToCsv extends OperationHandler<LdbcQuery2> {
        @Override
        protected OperationResult executeOperation(LdbcQuery2 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery3ToCsv extends OperationHandler<LdbcQuery3> {
        @Override
        protected OperationResult executeOperation(LdbcQuery3 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }


    public static class LdbcQuery4ToCsv extends OperationHandler<LdbcQuery4> {
        @Override
        protected OperationResult executeOperation(LdbcQuery4 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery5ToCsv extends OperationHandler<LdbcQuery5> {
        @Override
        protected OperationResult executeOperation(LdbcQuery5 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery6ToCsv extends OperationHandler<LdbcQuery6> {
        @Override
        protected OperationResult executeOperation(LdbcQuery6 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery7ToCsv extends OperationHandler<LdbcQuery7> {
        @Override
        protected OperationResult executeOperation(LdbcQuery7 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery8ToCsv extends OperationHandler<LdbcQuery8> {
        @Override
        protected OperationResult executeOperation(LdbcQuery8 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery9ToCsv extends OperationHandler<LdbcQuery9> {
        @Override
        protected OperationResult executeOperation(LdbcQuery9 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery10ToCsv extends OperationHandler<LdbcQuery10> {
        @Override
        protected OperationResult executeOperation(LdbcQuery10 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery11ToCsv extends OperationHandler<LdbcQuery11> {
        @Override
        protected OperationResult executeOperation(LdbcQuery11 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery12ToCsv extends OperationHandler<LdbcQuery12> {
        @Override
        protected OperationResult executeOperation(LdbcQuery12 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }
}
