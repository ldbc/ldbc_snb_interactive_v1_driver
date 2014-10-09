package com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.util.Map;

public class LdbcSnbFrequencyConverter {
    public static void convertFrequency(Map<String, String> params){
        Integer updateDistance = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.UPDATE_INTERLEAVE));

        Integer interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_1_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_2_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_3_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_4_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_5_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_6_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_7_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_8_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_9_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_10_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_11_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_12_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_13_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY,interleave.toString());

        interleave = Integer.parseInt(params.get(LdbcSnbInteractiveWorkload.READ_OPERATION_14_FREQUENCY_KEY)) * updateDistance;
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY,interleave.toString());

    }
}
