package com.ldbc.driver.workloads.ldbc.socnet.interactive.db;

import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;

import java.util.Map;

public class NothingDb extends Db {
    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        registerOperationHandler(LdbcQuery1.class, LdbcQuery1ToNothing.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2ToNothing.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3ToNothing.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4ToNothing.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5ToNothing.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6ToNothing.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7ToNothing.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8ToNothing.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9ToNothing.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10ToNothing.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11ToNothing.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12ToNothing.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13ToNothing.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14ToNothing.class);
    }

    @Override
    protected void onCleanup() throws DbException {
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return null;
    }

    public static class LdbcQuery1ToNothing extends OperationHandler<LdbcQuery1> {
        @Override
        protected OperationResult executeOperation(LdbcQuery1 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery2ToNothing extends OperationHandler<LdbcQuery2> {
        @Override
        protected OperationResult executeOperation(LdbcQuery2 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery3ToNothing extends OperationHandler<LdbcQuery3> {
        @Override
        protected OperationResult executeOperation(LdbcQuery3 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }


    public static class LdbcQuery4ToNothing extends OperationHandler<LdbcQuery4> {
        @Override
        protected OperationResult executeOperation(LdbcQuery4 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery5ToNothing extends OperationHandler<LdbcQuery5> {
        @Override
        protected OperationResult executeOperation(LdbcQuery5 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery6ToNothing extends OperationHandler<LdbcQuery6> {
        @Override
        protected OperationResult executeOperation(LdbcQuery6 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery7ToNothing extends OperationHandler<LdbcQuery7> {
        @Override
        protected OperationResult executeOperation(LdbcQuery7 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery8ToNothing extends OperationHandler<LdbcQuery8> {
        @Override
        protected OperationResult executeOperation(LdbcQuery8 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery9ToNothing extends OperationHandler<LdbcQuery9> {
        @Override
        protected OperationResult executeOperation(LdbcQuery9 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery10ToNothing extends OperationHandler<LdbcQuery10> {
        @Override
        protected OperationResult executeOperation(LdbcQuery10 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery11ToNothing extends OperationHandler<LdbcQuery11> {
        @Override
        protected OperationResult executeOperation(LdbcQuery11 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery12ToNothing extends OperationHandler<LdbcQuery12> {
        @Override
        protected OperationResult executeOperation(LdbcQuery12 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery13ToNothing extends OperationHandler<LdbcQuery13> {
        @Override
        protected OperationResult executeOperation(LdbcQuery13 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcQuery14ToNothing extends OperationHandler<LdbcQuery14> {
        @Override
        protected OperationResult executeOperation(LdbcQuery14 operation) throws DbException {
            return operation.buildResult(0, null);
        }
    }
}
