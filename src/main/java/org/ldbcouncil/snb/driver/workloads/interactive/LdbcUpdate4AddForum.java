package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcUpdate4AddForum.java
 * 
 * Interactive workload insert query 4:
 * -- Add forum --
 * 
 * Add a Forum node, connected to the network by 2 possible edge types.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.util.ListUtils;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LdbcUpdate4AddForum extends Operation<LdbcNoResult>
{
    public static final int TYPE = 1004;
    public static final String FORUM_ID = "forumId";
    public static final String FORUM_TITLE = "forumTitle";
    public static final String CREATION_DATE = "creationDate";
    public static final String MODERATOR_PERSON_ID = "moderatorPersonId";
    public static final String TAG_IDS = "tagIds";

    private final long forumId;
    private final String forumTitle;
    private final Date creationDate;
    private final long moderatorPersonId;
    private final List<Long> tagIds;

    public LdbcUpdate4AddForum(
        @JsonProperty("forumId")           long forumId,
        @JsonProperty("forumTitle")        String forumTitle,
        @JsonProperty("creationDate")      Date creationDate,
        @JsonProperty("moderatorPersonId") long moderatorPersonId,
        @JsonProperty("tagIds")            List<Long> tagIds
    )
    {
        this.forumId = forumId;
        this.forumTitle = forumTitle;
        this.creationDate = creationDate;
        this.moderatorPersonId = moderatorPersonId;
        this.tagIds = tagIds;
    }

    public long getForumId()
    {
        return forumId;
    }

    public String getForumTitle()
    {
        return forumTitle;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    public long getModeratorPersonId()
    {
        return moderatorPersonId;
    }

    public List<Long> getTagIds()
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

        LdbcUpdate4AddForum that = (LdbcUpdate4AddForum) o;

        if ( forumId != that.forumId )
        { return false; }
        if ( moderatorPersonId != that.moderatorPersonId )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }
        if ( forumTitle != null ? !forumTitle.equals( that.forumTitle ) : that.forumTitle != null )
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
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (forumTitle != null ? forumTitle.hashCode() : 0);
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (int) (moderatorPersonId ^ (moderatorPersonId >>> 32));
        result = 31 * result + (tagIds != null ? tagIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate4AddForum{" +
               "forumId=" + forumId +
               ", forumTitle='" + forumTitle + '\'' +
               ", creationDate=" + creationDate +
               ", moderatorPersonId=" + moderatorPersonId +
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
