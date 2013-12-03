package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery2Result
{
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long postId;
    private final String postContent;
    private final long postDate;

    public LdbcQuery2Result( long personId, String personFirstName, String personLastName, long postId,
            String postContent, long postDate )
    {
        super();
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.postId = postId;
        this.postContent = postContent;
        this.postDate = postDate;
    }

    public long personId()
    {
        return personId;
    }

    public String personFirstName()
    {
        return personFirstName;
    }

    public String personLastName()
    {
        return personLastName;
    }

    public long postId()
    {
        return postId;
    }

    public String postContent()
    {
        return postContent;
    }

    public long postDate()
    {
        return postDate;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( personFirstName == null ) ? 0 : personFirstName.hashCode() );
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        result = prime * result + ( ( personLastName == null ) ? 0 : personLastName.hashCode() );
        result = prime * result + ( ( postContent == null ) ? 0 : postContent.hashCode() );
        result = prime * result + (int) ( postDate ^ ( postDate >>> 32 ) );
        result = prime * result + (int) ( postId ^ ( postId >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery2Result other = (LdbcQuery2Result) obj;
        if ( personFirstName == null )
        {
            if ( other.personFirstName != null ) return false;
        }
        else if ( !personFirstName.equals( other.personFirstName ) ) return false;
        if ( personId != other.personId ) return false;
        if ( personLastName == null )
        {
            if ( other.personLastName != null ) return false;
        }
        else if ( !personLastName.equals( other.personLastName ) ) return false;
        if ( postContent == null )
        {
            if ( other.postContent != null ) return false;
        }
        else if ( !postContent.equals( other.postContent ) ) return false;
        if ( postDate != other.postDate ) return false;
        if ( postId != other.postId ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery2Result [personId=" + personId + ", personFirstName=" + personFirstName + ", personLastName="
               + personLastName + ", postId=" + postId + ", postContent=" + postContent + ", postDate=" + postDate
               + "]";
    }
}
