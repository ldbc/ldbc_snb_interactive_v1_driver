package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.util.ListUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

public class LdbcUpdate7AddComment extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 1007;
    private final long commentId;
    private final Date creationDate;
    private final String locationIp;
    private final String browserUsed;
    private final String content;
    private final int length;
    private final long authorPersonId;
    private final long countryId;
    private final long replyToPostId;
    private final long replyToCommentId;
    private final List<Long> tagIds;

    public LdbcUpdate7AddComment( long commentId,
            Date creationDate,
            String locationIp,
            String browserUsed,
            String content,
            int length,
            long authorPersonId,
            long countryId,
            long replyToPostId,
            long replyToCommentId,
            List<Long> tagIds )
    {
        this.commentId = commentId;
        this.creationDate = creationDate;
        this.locationIp = locationIp;
        this.browserUsed = browserUsed;
        this.content = content;
        this.length = length;
        this.authorPersonId = authorPersonId;
        this.countryId = countryId;
        this.replyToPostId = replyToPostId;
        this.replyToCommentId = replyToCommentId;
        this.tagIds = tagIds;
    }

    public long commentId()
    {
        return commentId;
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

    public long countryId()
    {
        return countryId;
    }

    public long replyToPostId()
    {
        return replyToPostId;
    }

    public long replyToCommentId()
    {
        return replyToCommentId;
    }

    public List<Long> tagIds()
    {
        return tagIds;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcUpdate7AddComment that = (LdbcUpdate7AddComment) o;

        if ( authorPersonId != that.authorPersonId )
        { return false; }
        if ( commentId != that.commentId )
        { return false; }
        if ( countryId != that.countryId )
        { return false; }
        if ( length != that.length )
        { return false; }
        if ( replyToCommentId != that.replyToCommentId )
        { return false; }
        if ( replyToPostId != that.replyToPostId )
        { return false; }
        if ( browserUsed != null ? !browserUsed.equals( that.browserUsed ) : that.browserUsed != null )
        { return false; }
        if ( content != null ? !content.equals( that.content ) : that.content != null )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
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
        int result = (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (locationIp != null ? locationIp.hashCode() : 0);
        result = 31 * result + (browserUsed != null ? browserUsed.hashCode() : 0);
        result = 31 * result + (content != null ? content.hashCode() : 0);
        result = 31 * result + length;
        result = 31 * result + (int) (authorPersonId ^ (authorPersonId >>> 32));
        result = 31 * result + (int) (countryId ^ (countryId >>> 32));
        result = 31 * result + (int) (replyToPostId ^ (replyToPostId >>> 32));
        result = 31 * result + (int) (replyToCommentId ^ (replyToCommentId >>> 32));
        result = 31 * result + (tagIds != null ? tagIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate7AddComment{" +
               "commentId=" + commentId +
               ", creationDate=" + creationDate +
               ", locationIp='" + locationIp + '\'' +
               ", browserUsed='" + browserUsed + '\'' +
               ", content='" + content + '\'' +
               ", length=" + length +
               ", authorPersonId=" + authorPersonId +
               ", countryId=" + countryId +
               ", replyToPostId=" + replyToPostId +
               ", replyToCommentId=" + replyToCommentId +
               ", tagIds=" + tagIds +
               '}';
    }

    @Override
    public void writeKyro( Kryo kryo, Output output ) {
        output.writeInt( type() );
        output.writeLong( commentId );
        output.writeLong( creationDate.getTime() );
        output.writeString( locationIp );
        output.writeString( browserUsed );
        output.writeString( content );
        output.writeInt( length );
        output.writeLong( authorPersonId );
        output.writeLong( countryId );
        output.writeLong( replyToPostId );
        output.writeLong( replyToCommentId);
        output.writeInt( tagIds.size() );
        for (Long tagId : tagIds) {
            output.writeLong( tagId );
        }
    }

    public static Operation readKyro( Input input ) {
        List<Long> tagIds = new ArrayList<>();
        Long commentId = input.readLong();
        Date creationDate = new Date( input.readLong() );
        String locationIp = input.readString();
        String browserUsed = input.readString();
        String content = input.readString();
        int length = input.readInt();
        Long authorPersonId = input.readLong();
        Long countryId = input.readLong();
        Long replyToPostId = input.readLong();
        Long replyToCommentId = input.readLong();
        int n = input.readInt();
        for (int i = 0; i < n; ++i) {
            tagIds.add( input.readLong() );
        }
        return new LdbcUpdate7AddComment(
            commentId,
            creationDate,
            locationIp,
            browserUsed,
            content,
            length,
            authorPersonId,
            countryId,
            replyToPostId,
            replyToCommentId,
            tagIds );
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
