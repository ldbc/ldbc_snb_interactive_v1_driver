package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery22InternationalDialogResult
{
    private final long personId1;
    private final long personId2;
    private final String city1Name;
    private final int score;

    public LdbcSnbBiQuery22InternationalDialogResult( long personId1, long personId2, String city1Name, int score )
    {
        this.personId1 = personId1;
        this.personId2 = personId2;
        this.city1Name = city1Name;
        this.score = score;
    }

    public long personId1()
    {
        return personId1;
    }

    public long personId2()
    {
        return personId2;
    }

    public String city1Name() {
        return city1Name;
    }

    public int score()
    {
        return score;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery22InternationalDialogResult{" +
                "personId1=" + personId1 +
                ", personId2=" + personId2 +
                ", city1Name='" + city1Name + '\'' +
                ", score=" + score +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery22InternationalDialogResult that = (LdbcSnbBiQuery22InternationalDialogResult) o;

        if (personId1 != that.personId1) return false;
        if (personId2 != that.personId2) return false;
        if (score != that.score) return false;
        return city1Name != null ? city1Name.equals(that.city1Name) : that.city1Name == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId1 ^ (personId1 >>> 32));
        result = 31 * result + (int) (personId2 ^ (personId2 >>> 32));
        result = 31 * result + (city1Name != null ? city1Name.hashCode() : 0);
        result = 31 * result + score;
        return result;
    }
}
