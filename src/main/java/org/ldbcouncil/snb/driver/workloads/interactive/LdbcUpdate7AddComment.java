package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcUpdate7AddComment.java
 * 
 * Interactive workload insert query 7:
 * -- Add comment --
 * 
 * Add a Comment node replying to a Post/Comment, connected to the network
 * by 4 possible edge types (replyOf, hasCreator, isLocatedIn, hasTag).
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.util.ListUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class LdbcUpdate7AddComment extends Operation<LdbcNoResult>
{
    public static final int TYPE = 1007;
    public static final String COMMENT_ID = "commentId";
    public static final String CREATION_DATE = "creationDate";
    public static final String LOCATION_IP = "locationIP";
    public static final String BROWSER_USED = "browserUsed";
    public static final String CONTENT = "content";
    public static final String LENGTH = "length";
    public static final String AUTHOR_PERSON_ID = "authorPersonId";
    public static final String COUNTRY_ID = "countryId";
    public static final String REPLY_TO_POST_ID = "replyToPostId";
    public static final String REPLY_TO_COMMENT_ID = "replyToCommentId";
    public static final String TAG_IDS = "tagIds";

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

    public LdbcUpdate7AddComment(
        @JsonProperty("commentId")              long commentId,
        @JsonProperty("creationDate")           Date creationDate,
        @JsonProperty("locationIp")             String locationIp,
        @JsonProperty("browserUsed")            String browserUsed,
        @JsonProperty("content")                String content,
        @JsonProperty("length")                 int length,
        @JsonProperty("tagIdauthorPersonIds")   long authorPersonId,
        @JsonProperty("countryId")              long countryId,
        @JsonProperty("replyToPostId")          long replyToPostId,
        @JsonProperty("tagreplyToCommentIdIds") long replyToCommentId,
        @JsonProperty("tagIds")                 List<Long> tagIds )
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

    public long getCommentId()
    {
        return commentId;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    public String getLocationIp()
    {
        return locationIp;
    }

    public String getBrowserUsed()
    {
        return browserUsed;
    }

    public String getContent()
    {
        return content;
    }

    public int getLength()
    {
        return length;
    }

    public long getAuthorPersonId()
    {
        return authorPersonId;
    }

    public long getCountryId()
    {
        return countryId;
    }

    public long getReplyToPostId()
    {
        return replyToPostId;
    }

    public long getReplyToCommentId()
    {
        return replyToCommentId;
    }

    public List<Long> getTagIds()
    {
        return tagIds;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COMMENT_ID, commentId)
                .put(CREATION_DATE, creationDate)
                .put(LOCATION_IP, locationIp)
                .put(BROWSER_USED, browserUsed)
                .put(CONTENT, content)
                .put(LENGTH, length)
                .put(AUTHOR_PERSON_ID, authorPersonId)
                .put(COUNTRY_ID, countryId)
                .put(REPLY_TO_POST_ID, replyToPostId)
                .put(REPLY_TO_COMMENT_ID, replyToCommentId)
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
        if ( tagIds != null ? !ListUtils.listsEqual( tagIds, that.tagIds ) : that.tagIds != null )
        { return false; }

        return true;
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
    public LdbcNoResult deserializeResult( String serializedResults )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
