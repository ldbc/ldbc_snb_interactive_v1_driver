package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.ldbc.driver.*;
import com.ldbc.driver.util.CsvFileWriter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.File;
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
            csvDbConnectionState = new CsvDbConnectionState(new CsvFileWriter(csvFile, CsvFileWriter.DEFAULT_COLUMN_SEPARATOR_STRING));
        } catch (IOException e) {
            throw new DbException("Error encountered while trying to create CSV file writer", e);
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
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13ToCsv.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14ToCsv.class);
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonToCsv.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeToCsv.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeToCsv.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumToCsv.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipToCsv.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostToCsv.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentToCsv.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipToCsv.class);
    }

    @Override
    protected void onCleanup() throws DbException {
        try {
            csvDbConnectionState.csvFileWriter().close();
        } catch (IOException e) {
            throw new DbException("Error encountered while trying to close CSV file writer", e);
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

    public static class LdbcQuery1ToCsv extends OperationHandler<LdbcQuery1> {
        static final List<LdbcQuery1Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery1 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.firstName());
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery2ToCsv extends OperationHandler<LdbcQuery2> {
        static final List<LdbcQuery2Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery2 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.maxDate().toString());
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery3ToCsv extends OperationHandler<LdbcQuery3> {
        static final List<LdbcQuery3Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery3 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.countryXName(),
                        operation.countryYName(),
                        operation.startDate().toString(),
                        Integer.toString(operation.durationDays()));
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }


    public static class LdbcQuery4ToCsv extends OperationHandler<LdbcQuery4> {
        static final List<LdbcQuery4Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery4 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.startDate().toString(),
                        Integer.toString(operation.durationDays()));
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery5ToCsv extends OperationHandler<LdbcQuery5> {
        static final List<LdbcQuery5Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery5 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.minDate().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery6ToCsv extends OperationHandler<LdbcQuery6> {
        static final List<LdbcQuery6Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery6 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.tagName()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery7ToCsv extends OperationHandler<LdbcQuery7> {
        static final List<LdbcQuery7Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery7 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery8ToCsv extends OperationHandler<LdbcQuery8> {
        static final List<LdbcQuery8Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery8 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery9ToCsv extends OperationHandler<LdbcQuery9> {
        static final List<LdbcQuery9Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery9 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.maxDate().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery10ToCsv extends OperationHandler<LdbcQuery10> {
        static final List<LdbcQuery10Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery10 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        Integer.toString(operation.month1()),
                        Integer.toString(operation.month2())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery11ToCsv extends OperationHandler<LdbcQuery11> {
        static final List<LdbcQuery11Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery11 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.countryName(),
                        Integer.toString(operation.workFromYear())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery12ToCsv extends OperationHandler<LdbcQuery12> {
        static final List<LdbcQuery12Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery12 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personUri(),
                        operation.tagClassName()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery13ToCsv extends OperationHandler<LdbcQuery13> {
        static final List<LdbcQuery13Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery13 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.person1Id()),
                        Long.toString(operation.person2Id()),
                        operation.person1Uri(),
                        operation.person2Uri()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcQuery14ToCsv extends OperationHandler<LdbcQuery14> {
        static final List<LdbcQuery14Result> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcQuery14 operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.person1Id()),
                        Long.toString(operation.person2Id()),
                        operation.person1Uri(),
                        operation.person2Uri()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate1AddPersonToCsv extends OperationHandler<LdbcUpdate1AddPerson> {
        static final List<LdbcUpdate1AddPerson> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate1AddPerson operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        operation.personFirstName(),
                        operation.personLastName(),
                        operation.gender(),
                        Long.toString(operation.birthday().getTime()),
                        Long.toString(operation.creationDate().getTime()),
                        operation.locationIp(),
                        operation.browserUsed(),
                        Long.toString(operation.cityId()),
                        operation.languages().toString(),
                        operation.emails().toString(),
                        operation.tagIds().toString(),
                        operation.studyAt().toString(),
                        operation.workAt().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate2AddPostLikeToCsv extends OperationHandler<LdbcUpdate2AddPostLike> {
        static final List<LdbcUpdate2AddPostLike> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate2AddPostLike operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        Long.toString(operation.postId()),
                        Long.toString(operation.creationDate().getTime())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate3AddCommentLikeToCsv extends OperationHandler<LdbcUpdate3AddCommentLike> {
        static final List<LdbcUpdate3AddCommentLike> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate3AddCommentLike operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.personId()),
                        Long.toString(operation.commentId()),
                        Long.toString(operation.creationDate().getTime())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate4AddForumToCsv extends OperationHandler<LdbcUpdate4AddForum> {
        static final List<LdbcUpdate4AddForum> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate4AddForum operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.forumId()),
                        operation.forumTitle(),
                        Long.toString(operation.creationDate().getTime()),
                        Long.toString(operation.moderatorPersonId()),
                        operation.tagIds().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate5AddForumMembershipToCsv extends OperationHandler<LdbcUpdate5AddForumMembership> {
        static final List<LdbcUpdate5AddForumMembership> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate5AddForumMembership operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.forumId()),
                        Long.toString(operation.personId()),
                        Long.toString(operation.creationDate().getTime())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate6AddPostToCsv extends OperationHandler<LdbcUpdate6AddPost> {
        static final List<LdbcUpdate6AddPost> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate6AddPost operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.postId()),
                        operation.imageFile(),
                        Long.toString(operation.creationDate().getTime()),
                        operation.locationIp(),
                        operation.browserUsed(),
                        operation.language(),
                        operation.content(),
                        Integer.toString(operation.length()),
                        Long.toString(operation.authorPersonId()),
                        Long.toString(operation.forumId()),
                        Long.toString(operation.countryId()),
                        operation.tagIds().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate7AddCommentToCsv extends OperationHandler<LdbcUpdate7AddComment> {
        static final List<LdbcUpdate7AddComment> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate7AddComment operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.commentId()),
                        Long.toString(operation.creationDate().getTime()),
                        operation.locationIp(),
                        operation.browserUsed(),
                        operation.content(),
                        Integer.toString(operation.length()),
                        Long.toString(operation.authorPersonId()),
                        Long.toString(operation.countryId()),
                        Long.toString(operation.replyToPostId()),
                        Long.toString(operation.replyToCommentId()),
                        operation.tagIds().toString()
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }

    public static class LdbcUpdate8AddFriendshipToCsv extends OperationHandler<LdbcUpdate8AddFriendship> {
        static final List<LdbcUpdate8AddFriendship> RESULT = new ArrayList<>();

        @Override
        protected OperationResultReport executeOperation(LdbcUpdate8AddFriendship operation) throws DbException {
            try {
                ((CsvDbConnectionState) dbConnectionState()).csvFileWriter().writeRow(
                        operation.type(),
                        Long.toString(operation.person1Id()),
                        Long.toString(operation.person2Id()),
                        Long.toString(operation.creationDate().getTime())
                );
                return operation.buildResult(0, RESULT);
            } catch (IOException e) {
                throw new DbException(String.format("Error encountered while writing to CSV file: %s", operation.toString()), e);
            }
        }
    }
}
