package com.ldbc.driver.csv;

import com.ldbc.driver.csv.charseeker.*;
import org.junit.Test;

import java.io.StringReader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExtractorsTest {
    @Test
    public void shouldParseEmptyIntTupleArray() throws Exception {
        int[] columnDelimiters = new int[]{'|'};
        String data = "";
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));

        Mark mark = new Mark();

        assertThat(charSeeker.seek(mark, columnDelimiters), is(false));
    }

    @Test
    public void shouldParseIntTupleArrayAsSingleColumnSingleTuple() throws Exception {
        int[] columnDelimiters = new int[]{'|'};
        String data = "1,2";
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));

        char arrayDelimiter = ';';
        char tupleDelimiter = ',';
        int tupleLength = 2;
        Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
        Mark mark = new Mark();

        charSeeker.seek(mark, columnDelimiters);
        int[][] intTupleArray = charSeeker.extract(mark, extractors.intTupleArray(2)).value();
        assertThat(intTupleArray.length, is(1));
        assertThat(intTupleArray[0], equalTo(new int[]{1, 2}));
    }

    @Test
    public void shouldParseIntTupleArrayAsSingleColumnManyTuples() throws Exception {
        int[] columnDelimiters = new int[]{'|'};
        String data = "1,2;3,4;5,6;7,8";
        CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));

        char arrayDelimiter = ';';
        char tupleDelimiter = ',';
        int tupleLength = 2;
        Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
        Mark mark = new Mark();

        charSeeker.seek(mark, columnDelimiters);
        int[][] intTupleArray = charSeeker.extract(mark, extractors.intTupleArray(2)).value();
        assertThat(intTupleArray.length, is(4));
        assertThat(intTupleArray[0], equalTo(new int[]{1, 2}));
        assertThat(intTupleArray[1], equalTo(new int[]{3, 4}));
        assertThat(intTupleArray[2], equalTo(new int[]{5, 6}));
        assertThat(intTupleArray[3], equalTo(new int[]{7, 8}));
    }
}
