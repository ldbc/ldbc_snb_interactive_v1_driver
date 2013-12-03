package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery5Result
{
    private final String forumTitle;
    private final long postCount;
    private final long commentCount;

    public LdbcQuery5Result( String forumTitle, long postCount, long commentCount )
    {
        super();
        this.forumTitle = forumTitle;
        this.postCount = postCount;
        this.commentCount = commentCount;
    }

    public String forumTitle()
    {
        return forumTitle;
    }

    public long postCount()
    {
        return postCount;
    }

    public long commentCount()
    {
        return commentCount;
    }

    public long count()
    {
        return postCount + commentCount;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( commentCount ^ ( commentCount >>> 32 ) );
        result = prime * result + ( ( forumTitle == null ) ? 0 : forumTitle.hashCode() );
        result = prime * result + (int) ( postCount ^ ( postCount >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery5Result other = (LdbcQuery5Result) obj;
        if ( commentCount != other.commentCount ) return false;
        if ( forumTitle == null )
        {
            if ( other.forumTitle != null ) return false;
        }
        else if ( !forumTitle.equals( other.forumTitle ) ) return false;
        if ( postCount != other.postCount ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery5Result [forumTitle=" + forumTitle + ", postCount=" + postCount + ", commentCount="
               + commentCount + ", count=" + count() + "]";
    }
}
