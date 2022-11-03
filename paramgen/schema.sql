DROP TABLE IF EXISTS Q_1;
DROP TABLE IF EXISTS Q_2;
DROP TABLE IF EXISTS Q_3a;
DROP TABLE IF EXISTS Q_3b;
DROP TABLE IF EXISTS Q_4;
DROP TABLE IF EXISTS Q_5;
DROP TABLE IF EXISTS Q_6;
DROP TABLE IF EXISTS Q_7;
DROP TABLE IF EXISTS Q_8;
DROP TABLE IF EXISTS Q_9;
DROP TABLE IF EXISTS Q_10;
DROP TABLE IF EXISTS Q_11;
DROP TABLE IF EXISTS Q_12;
DROP TABLE IF EXISTS Q_13a;
DROP TABLE IF EXISTS Q_13b;
DROP TABLE IF EXISTS Q_14a;
DROP TABLE IF EXISTS Q_14b;
DROP TABLE IF EXISTS Q_personId;
DROP TABLE IF EXISTS Q_messageId;

CREATE TABLE Q_1 (
    personId bigint,
    firstName varchar,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_2 (
    personId bigint,
    maxDate timestamp,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_3a (
    personId bigint,
    countryXName varchar,
    countryYName varchar,
    startDate timestamp,
    durationDays bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_3b (
    personId bigint,
    countryXName varchar,
    countryYName varchar,
    startDate timestamp,
    durationDays bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_4 (
    personId bigint,
    startDate timestamp,
    durationDays bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_5 (
    personId bigint,
    minDate timestamp,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_6 (
    personId bigint,
    tagName varchar,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_7 (
    personId bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_8 (
    personId bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_9 (
    personId bigint,
    maxDate timestamp,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_10 (
    personId bigint,
    month bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_11 (
    personId bigint,
    countryName varchar,
    workFromYear bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_12 (
    personId bigint,
    tagClassName varchar,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_13a (
    person1Id bigint,
    person2Id bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_13b (
    person1Id bigint,
    person2Id bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_14a (
    person1Id bigint,
    person2Id bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_14b (
    person1Id bigint,
    person2Id bigint,
    useFrom timestamp,
    useUntil timestamp
);

CREATE TABLE Q_personId (
    personId bigint,
);

CREATE TABLE Q_messageId (
    MessageId bigint,
);
