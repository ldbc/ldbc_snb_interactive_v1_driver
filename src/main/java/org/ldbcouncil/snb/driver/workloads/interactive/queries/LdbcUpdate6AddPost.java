package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcUpdate6AddPost.java
 * 
 * Interactive workload insert query 6:
 * -- Add post --
 * 
 * Add a Post node to the social network connected by 4 possible edge types
 * (hasCreator, containerOf, isLocatedIn, hasTag).
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import org.ldbcouncil.snb.driver.util.ListUtils;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LdbcUpdate6AddPost extends LdbcOperation<LdbcNoResult>
{
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

    public LdbcUpdate6AddPost( 
        @JsonProperty("postId")         long postId,
        @JsonProperty("imageFile")      String imageFile,
        @JsonProperty("creationDate")   Date creationDate,
        @JsonProperty("locationIp")     String locationIp,
        @JsonProperty("browserUsed")    String browserUsed,
        @JsonProperty("language")       String language,
        @JsonProperty("content")        String content,
        @JsonProperty("length")         int length,
        @JsonProperty("authorPersonId") long authorPersonId,
        @JsonProperty("forumId")        long forumId,
        @JsonProperty("countryId")      long countryId,
        @JsonProperty("tagIds")         List<Long> tagIds
    )
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

    public long getPostId()
    {
        return postId;
    }

    public String getImageFile()
    {
        return imageFile;
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

    public String getLanguage()
    {
        return language;
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

    public long getForumId()
    {
        return forumId;
    }

    public long getCountryId()
    {
        return countryId;
    }

    public List<Long> getTagIds()
    {
        return tagIds;
    }

    @Override
    public Map<String, Object> parameterMap() {
        // use vanilla HashMap to allow null values
        final HashMap<String, Object> parameterMap = new HashMap<>();
        parameterMap.put(POST_ID, postId);
        parameterMap.put(IMAGE_FILE, imageFile); // can be null
        parameterMap.put(CREATION_DATE, creationDate);
        parameterMap.put(LOCATION_IP, locationIp);
        parameterMap.put(BROWSER_USED, browserUsed);
        parameterMap.put(LANGUAGE, language);
        parameterMap.put(CONTENT, content);
        parameterMap.put(LENGTH, length);
        parameterMap.put(AUTHOR_PERSON_ID, authorPersonId);
        parameterMap.put(FORUM_ID, forumId);
        parameterMap.put(COUNTRY_ID, countryId);
        parameterMap.put(TAG_IDS, tagIds);
        return parameterMap;
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
        if ( tagIds != null ? !ListUtils.listsEqual( tagIds , that.tagIds ) : that.tagIds != null )
        { return false; }

        return true;
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
