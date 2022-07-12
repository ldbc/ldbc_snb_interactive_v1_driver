package org.ldbcouncil.snb.driver.validation;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.testutils.TestUtils;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveDb;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveOperationResultInstances;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ValidationParamsToCsvRowsToValidationParamsTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void validationParametersShouldBeUnchangedAfterSerializingAndMarshalling()
            throws IOException, DriverConfigurationException, WorkloadException
    {
        long operationCount = 1;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( dbClassName, workloadClassName, operationCount );

        Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( paramsMap );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                MapUtils.loadPropertiesToMap( TestUtils.getResource( "/snb/interactive/updateStream.properties" ) )
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        // (1) params
        List<ValidationParam> validationParamsBeforeSerializing =
                buildParams( workload.dbValidationParametersFilter( 0 ) );

        // (2) original->JSON
        File jsonFile1 = temporaryFolder.newFile();
        ValidationParamsToJson validationParamsAsJson = 
            new ValidationParamsToJson( validationParamsBeforeSerializing, workload, true );
        validationParamsAsJson.serializeValidationParameters(jsonFile1);

        // (3) JSON->params
        ValidationParamsFromJson validationParamsFromJson = new ValidationParamsFromJson(jsonFile1, workload);
        List<ValidationParam> validationParamsAfterDeserializing = validationParamsFromJson.deserialize();
      
        // Then
        assertThat( validationParamsBeforeSerializing, equalTo( validationParamsAfterDeserializing ) );
    }

    List<ValidationParam> buildParams( Workload.DbValidationParametersFilter dbValidationParametersFilter )
    {
        LdbcQuery1 read1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        List<LdbcQuery1Result> readResult1 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read1Result()
        );
        ValidationParam validationParamRead1 = ValidationParam.createTyped( read1, readResult1 );

        LdbcQuery2 read2 = DummyLdbcSnbInteractiveOperationInstances.read2();
        List<LdbcQuery2Result> readResult2 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read2Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read2Result()
        );
        ValidationParam validationParamRead2 = ValidationParam.createTyped( read2, readResult2 );

        LdbcQuery3 read3 = DummyLdbcSnbInteractiveOperationInstances.read3();
        List<LdbcQuery3Result> readResult3 = Lists.newArrayList();
        ValidationParam validationParamRead3 = ValidationParam.createTyped( read3, readResult3 );

        LdbcQuery4 read4 = DummyLdbcSnbInteractiveOperationInstances.read4();
        List<LdbcQuery4Result> readResult4 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result()
        );
        ValidationParam validationParamRead4 = ValidationParam.createTyped( read4, readResult4 );

        LdbcQuery5 read5 = DummyLdbcSnbInteractiveOperationInstances.read5();
        List<LdbcQuery5Result> readResult5 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read5Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read5Result()
        );
        ValidationParam validationParamRead5 = ValidationParam.createTyped( read5, readResult5 );

        LdbcQuery6 read6 = DummyLdbcSnbInteractiveOperationInstances.read6();
        List<LdbcQuery6Result> readResult6 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result()
        );
        ValidationParam validationParamRead6 = ValidationParam.createTyped( read6, readResult6 );

        LdbcQuery7 read7 = DummyLdbcSnbInteractiveOperationInstances.read7();
        List<LdbcQuery7Result> readResult7 = Lists.newArrayList(
        );
        ValidationParam validationParamRead7 = ValidationParam.createTyped( read7, readResult7 );

        LdbcQuery8 read8 = DummyLdbcSnbInteractiveOperationInstances.read8();
        List<LdbcQuery8Result> readResult8 = Lists.newArrayList(
        );
        ValidationParam validationParamRead8 = ValidationParam.createTyped( read8, readResult8 );

        LdbcQuery9 read9 = DummyLdbcSnbInteractiveOperationInstances.read9();
        List<LdbcQuery9Result> readResult9 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read9Result()
        );
        ValidationParam validationParamRead9 = ValidationParam.createTyped( read9, readResult9 );

        LdbcQuery10 read10 = DummyLdbcSnbInteractiveOperationInstances.read10();
        List<LdbcQuery10Result> readResult10 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read10Result()
        );
        ValidationParam validationParamRead10 = ValidationParam.createTyped( read10, readResult10 );

        LdbcQuery11 read11 = DummyLdbcSnbInteractiveOperationInstances.read11();
        List<LdbcQuery11Result> readResult11 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read11Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read11Result()
        );
        ValidationParam validationParamRead11 = ValidationParam.createTyped( read11, readResult11 );

        LdbcQuery12 read12 = DummyLdbcSnbInteractiveOperationInstances.read12();
        List<LdbcQuery12Result> readResult12 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read12Result()
        );
        ValidationParam validationParamRead12 = ValidationParam.createTyped( read12, readResult12 );

        LdbcQuery13 read13 = DummyLdbcSnbInteractiveOperationInstances.read13();
        LdbcQuery13Result readResult13 = DummyLdbcSnbInteractiveOperationResultInstances.read13Result();
        ValidationParam validationParamRead13 = ValidationParam.createTyped( read13, readResult13 );

        LdbcQuery14 read14 = DummyLdbcSnbInteractiveOperationInstances.read14();
        List<LdbcQuery14Result> readResult14 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read14Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read14Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read14Result()
        );
        ValidationParam validationParamRead14 = ValidationParam.createTyped( read14, readResult14 );

        LdbcShortQuery1PersonProfile shortRead1 = DummyLdbcSnbInteractiveOperationInstances.short1();
        LdbcShortQuery1PersonProfileResult shortReadResult1 =
                DummyLdbcSnbInteractiveOperationResultInstances.short1Result();
        ValidationParam validationParamShortRead1 = ValidationParam.createTyped( shortRead1, shortReadResult1 );

        LdbcShortQuery2PersonPosts shortRead2 = DummyLdbcSnbInteractiveOperationInstances.short2();
        List<LdbcShortQuery2PersonPostsResult> shortReadResult2 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.short2Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.short2Result()
        );
        ValidationParam validationParamShortRead2 = ValidationParam.createTyped( shortRead2, shortReadResult2 );

        LdbcShortQuery3PersonFriends shortRead3 = DummyLdbcSnbInteractiveOperationInstances.short3();
        List<LdbcShortQuery3PersonFriendsResult> shortReadResult3 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.short3Result()
        );
        ValidationParam validationParamShortRead3 = ValidationParam.createTyped( shortRead3, shortReadResult3 );

        LdbcShortQuery4MessageContent shortRead4 = DummyLdbcSnbInteractiveOperationInstances.short4();
        LdbcShortQuery4MessageContentResult shortReadResult4 =
                DummyLdbcSnbInteractiveOperationResultInstances.short4Result();
        ValidationParam validationParamShortRead4 = ValidationParam.createTyped( shortRead4, shortReadResult4 );

        LdbcShortQuery5MessageCreator shortRead5 = DummyLdbcSnbInteractiveOperationInstances.short5();
        LdbcShortQuery5MessageCreatorResult shortReadResult5 =
                DummyLdbcSnbInteractiveOperationResultInstances.short5Result();
        ValidationParam validationParamShortRead5 = ValidationParam.createTyped( shortRead5, shortReadResult5 );

        LdbcShortQuery6MessageForum shortRead6 = DummyLdbcSnbInteractiveOperationInstances.short6();
        LdbcShortQuery6MessageForumResult shortReadResult6 =
                DummyLdbcSnbInteractiveOperationResultInstances.short6Result();
        ValidationParam validationParamShortRead6 = ValidationParam.createTyped( shortRead6, shortReadResult6 );

        LdbcShortQuery7MessageReplies shortRead7 = DummyLdbcSnbInteractiveOperationInstances.short7();
        List<LdbcShortQuery7MessageRepliesResult> shortReadResult7 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
        );
        ValidationParam validationParamShortRead7 = ValidationParam.createTyped( shortRead7, shortReadResult7 );

        LdbcInsert1AddPerson write1 = DummyLdbcSnbInteractiveOperationInstances.write1();
        ValidationParam validationParamWrite1 = ValidationParam.createTyped( write1, LdbcNoResult.INSTANCE );

        LdbcInsert2AddPostLike write2 = DummyLdbcSnbInteractiveOperationInstances.write2();
        ValidationParam validationParamWrite2 = ValidationParam.createTyped( write2, LdbcNoResult.INSTANCE );

        LdbcInsert3AddCommentLike write3 = DummyLdbcSnbInteractiveOperationInstances.write3();
        ValidationParam validationParamWrite3 = ValidationParam.createTyped( write3, LdbcNoResult.INSTANCE );

        LdbcInsert4AddForum write4 = DummyLdbcSnbInteractiveOperationInstances.write4();
        ValidationParam validationParamWrite4 = ValidationParam.createTyped( write4, LdbcNoResult.INSTANCE );

        LdbcInsert5AddForumMembership write5 = DummyLdbcSnbInteractiveOperationInstances.write5();
        ValidationParam validationParamWrite5 = ValidationParam.createTyped( write5, LdbcNoResult.INSTANCE );

        LdbcInsert6AddPost write6 = DummyLdbcSnbInteractiveOperationInstances.write6();
        ValidationParam validationParamWrite6 = ValidationParam.createTyped( write6, LdbcNoResult.INSTANCE );

        LdbcInsert7AddComment write7 = DummyLdbcSnbInteractiveOperationInstances.write7();
        ValidationParam validationParamWrite7 = ValidationParam.createTyped( write7, LdbcNoResult.INSTANCE );

        LdbcInsert8AddFriendship write8 = DummyLdbcSnbInteractiveOperationInstances.write8();
        ValidationParam validationParamWrite8 = ValidationParam.createTyped( write8, LdbcNoResult.INSTANCE );

        LdbcDelete1RemovePerson delete1 = DummyLdbcSnbInteractiveOperationInstances.delete1();
        ValidationParam validationParamDelete1 = ValidationParam.createTyped( delete1, LdbcNoResult.INSTANCE );

        LdbcDelete2RemovePostLike delete2 = DummyLdbcSnbInteractiveOperationInstances.delete2();
        ValidationParam validationParamDelete2 = ValidationParam.createTyped( delete2, LdbcNoResult.INSTANCE );

        LdbcDelete3RemoveCommentLike delete3 = DummyLdbcSnbInteractiveOperationInstances.delete3();
        ValidationParam validationParamDelete3 = ValidationParam.createTyped( delete3, LdbcNoResult.INSTANCE );

        LdbcDelete4RemoveForum delete4 = DummyLdbcSnbInteractiveOperationInstances.delete4();
        ValidationParam validationParamDelete4 = ValidationParam.createTyped( delete4, LdbcNoResult.INSTANCE );

        LdbcDelete5RemoveForumMembership delete5 = DummyLdbcSnbInteractiveOperationInstances.delete5();
        ValidationParam validationParamDelete5 = ValidationParam.createTyped( delete5, LdbcNoResult.INSTANCE );

        LdbcDelete6RemovePostThread delete6 = DummyLdbcSnbInteractiveOperationInstances.delete6();
        ValidationParam validationParamDelete6 = ValidationParam.createTyped( delete6, LdbcNoResult.INSTANCE );

        LdbcDelete7RemoveCommentSubthread delete7 = DummyLdbcSnbInteractiveOperationInstances.delete7();
        ValidationParam validationParamDelete7 = ValidationParam.createTyped( delete7, LdbcNoResult.INSTANCE );

        LdbcDelete8RemoveFriendship delete8 = DummyLdbcSnbInteractiveOperationInstances.delete8();
        ValidationParam validationParamDelete8 = ValidationParam.createTyped( delete8, LdbcNoResult.INSTANCE );

        return Lists.newArrayList(
                // Long Reads
                validationParamRead1,
                validationParamRead2,
                validationParamRead3,
                validationParamRead4,
                validationParamRead5,
                validationParamRead6,
                validationParamRead7,
                validationParamRead8,
                validationParamRead9,
                validationParamRead10,
                validationParamRead11,
                validationParamRead12,
                validationParamRead13,
                validationParamRead14,
                // Short Reads
                validationParamShortRead1,
                validationParamShortRead2,
                validationParamShortRead3,
                validationParamShortRead4,
                validationParamShortRead5,
                validationParamShortRead6,
                validationParamShortRead7,
                // Writes
                validationParamWrite1,
                validationParamWrite2,
                validationParamWrite3,
                validationParamWrite4,
                validationParamWrite5,
                validationParamWrite6,
                validationParamWrite7,
                validationParamWrite8,
                // Deletes
                validationParamDelete1,
                validationParamDelete2,
                validationParamDelete3,
                validationParamDelete4,
                validationParamDelete5,
                validationParamDelete6,
                validationParamDelete7,
                validationParamDelete8
        );
    }
}
