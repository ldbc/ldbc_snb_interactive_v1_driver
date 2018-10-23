package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.util.ListUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class LdbcUpdate6AddPost extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 1006;
    public static final String POST_ID = "postId";
    public static final String IMAGE_FILE = "imageFile";
    public static final String CREATION_DATE = "creationDate";
    public static final String LOCATION_IP = "locationIP";
    public static final String BROWSER_USED = "browserUsed";
    public static final String LANGUAGE = "language";
    public static final String CONTENT = "content";
    public static final String LENGTH = "length";
    public static final String AUTHOR_PERSON_ID = "authorPersonId";
    public static final String FORUM_ID = "forumId";
    public static final String COUNTRY_ID = "countryId";
    public static final String TAG_IDS = "tagIds";

    private final long postId;
    private final String imageFile;
    private final Date creationDate;
    private final String locationIp;
    private final String browserUsed;
    private final String language;
    private final String content;
    private final int length;
    private final long authorPersonId;
    private final long forumId;
    private final long countryId;
    private final List<Long> tagIds;

    public LdbcUpdate6AddPost( long postId,
            String imageFile,
            Date creationDate,
            String locationIp,
            String browserUsed,
            String language,
            String content,
            int length,
            long authorPersonId,
            long forumId,
            long countryId,
            List<Long> tagIds )
    {
        this.postId = postId;
        this.imageFile = imageFile;
        this.creationDate = creationDate;
        this.locationIp = locationIp;
        this.browserUsed = browserUsed;
        this.language = language;
        this.content = content;
        this.length = length;
        this.authorPersonId = authorPersonId;
        this.forumId = forumId;
        this.countryId = countryId;
        this.tagIds = tagIds;
    }

    public long postId()
    {
        return postId;
    }

    public String imageFile()
    {
        return imageFile;
    }

    public Date creationDate()
    {
        return creationDate;
    }

    public String locationIp()
    {
        return locationIp;
    }

    public String browserUsed()
    {
        return browserUsed;
    }

    public String language()
    {
        return language;
    }

    public String content()
    {
        return content;
    }

    public int length()
    {
        return length;
    }

    public long authorPersonId()
    {
        return authorPersonId;
    }

    public long forumId()
    {
        return forumId;
    }

    public long countryId()
    {
        return countryId;
    }

    public List<Long> tagIds()
    {
        return tagIds;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(POST_ID, postId)
                .put(IMAGE_FILE, imageFile)
                .put(CREATION_DATE, creationDate)
                .put(LOCATION_IP, locationIp)
                .put(BROWSER_USED, browserUsed)
                .put(LANGUAGE, language)
                .put(CONTENT, content)
                .put(LENGTH, length)
                .put(AUTHOR_PERSON_ID, authorPersonId)
                .put(FORUM_ID, forumId)
                .put(COUNTRY_ID, countryId)
                .put(TAG_IDS, tagIds)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcUpdate6AddPost that = (LdbcUpdate6AddPost) o;

        if ( authorPersonId != that.authorPersonId )
        { return false; }
        if ( countryId != that.countryId )
        { return false; }
        if ( forumId != that.forumId )
        { return false; }
        if ( length != that.length )
        { return false; }
        if ( postId != that.postId )
        { return false; }
        if ( browserUsed != null ? !browserUsed.equals( that.browserUsed ) : that.browserUsed != null )
        { return false; }
        if ( content != null ? !content.equals( that.content ) : that.content != null )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }
        if ( imageFile != null ? !imageFile.equals( that.imageFile ) : that.imageFile != null )
        { return false; }
        if ( language != null ? !language.equals( that.language ) : that.language != null )
        { return false; }
        if ( locationIp != null ? !locationIp.equals( that.locationIp ) : that.locationIp != null )
        { return false; }
        if ( tagIds != null ? !ListUtils.listsEqual( sort( tagIds ), sort( that.tagIds ) ) : that.tagIds != null )
        { return false; }

        return true;
    }

    private <T extends Comparable> List<T> sort( List<T> list )
    {
        Collections.sort( list );
        return list;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (postId ^ (postId >>> 32));
        result = 31 * result + (imageFile != null ? imageFile.hashCode() : 0);
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (locationIp != null ? locationIp.hashCode() : 0);
        result = 31 * result + (browserUsed != null ? browserUsed.hashCode() : 0);
        result = 31 * result + (language != null ? language.hashCode() : 0);
        result = 31 * result + (content != null ? content.hashCode() : 0);
        result = 31 * result + length;
        result = 31 * result + (int) (authorPersonId ^ (authorPersonId >>> 32));
        result = 31 * result + (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (int) (countryId ^ (countryId >>> 32));
        result = 31 * result + (tagIds != null ? tagIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate6AddPost{" +
               "postId=" + postId +
               ", imageFile='" + imageFile + '\'' +
               ", creationDate=" + creationDate +
               ", locationIp='" + locationIp + '\'' +
               ", browserUsed='" + browserUsed + '\'' +
               ", language='" + language + '\'' +
               ", content='" + content + '\'' +
               ", length=" + length +
               ", authorPersonId=" + authorPersonId +
               ", forumId=" + forumId +
               ", countryId=" + countryId +
               ", tagIds=" + tagIds +
               '}';
    }

    @Override
    public LdbcNoResult marshalResult( String serializedOperationResult )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        try
        {
            return objectMapper.writeValueAsString(
                    LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    operationResultInstance ), e );
        }
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
