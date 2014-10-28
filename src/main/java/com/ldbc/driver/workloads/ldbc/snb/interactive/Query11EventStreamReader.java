package com.ldbc.driver.workloads.ldbc.snb.interactive;


import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;

import java.io.IOException;
import java.util.Iterator;

public class Query11EventStreamReader implements Iterator<Operation<?>> {
    private final Iterator<Object[]> csvRows;

    public Query11EventStreamReader(Iterator<Object[]> csvRows) {
        this.csvRows = csvRows;
    }

    @Override
    public boolean hasNext() {
        return csvRows.hasNext();
    }

    @Override
    public Operation<?> next() {
        Object[] rowAsObjects = csvRows.next();
        return new LdbcQuery11(
                (long) rowAsObjects[0],
                (String) rowAsObjects[1],
                (int) rowAsObjects[2],
                LdbcQuery11.DEFAULT_LIMIT
        );
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(String.format("%s does not support remove()", getClass().getSimpleName()));
    }

    public static class Query11Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> {
        /*
        Person|Country|Year
        2199032251700|Egypt|1995
        */
        @Override
        public Object[] decodeEvent(CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark) throws IOException {
            long personId;
            if (charSeeker.seek(mark, columnDelimiters)) {
                personId = charSeeker.extract(mark, extractors.long_()).longValue();
            } else {
                // if first column of next row contains nothing it means the file is finished
                return null;
            }

            String countryName;
            if (charSeeker.seek(mark, columnDelimiters)) {
                countryName = charSeeker.extract(mark, extractors.string()).value();
            } else {
                throw new GeneratorException("Error retrieving country name");
            }

            int year;
            if (charSeeker.seek(mark, columnDelimiters)) {
                year = charSeeker.extract(mark, extractors.int_()).intValue();
            } else {
                throw new GeneratorException("Error retrieving year");
            }

            return new Object[]{personId, countryName, year};
        }
    }
}
