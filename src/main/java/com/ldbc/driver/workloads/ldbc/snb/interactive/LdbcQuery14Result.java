package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery14Result
{
    private final String link;
    private final int linkCount;

    public LdbcQuery14Result(String link, int linkCount) {
        this.link = link;
        this.linkCount = linkCount;
    }

    public String link() {
        return link;
    }

    public int linkCount() {
        return linkCount;
    }

    @Override
    public boolean equals( Object o )
    {
	if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14Result that = (LdbcQuery14Result) o;

        if (linkCount != that.linkCount) return false;
        if (link != null ? !link.equals(that.link) : that.link != null) return false;

        return true;
    }

    @Override
    public String toString()
    {
	return "LdbcQuery14Result{" +
                "link='" + link + '\'' +
                ", linkCount=" + linkCount +
                '}';
    }
}
