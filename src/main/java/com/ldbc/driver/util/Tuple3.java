package com.ldbc.driver.util;

public class Tuple3<T1, T2, T3>
{
    private final T1 thing1;
    private final T2 thing2;
    private final T3 thing3;

    public Tuple3( T1 t1, T2 t2, T3 t3 )
    {
        thing1 = t1;
        thing2 = t2;
        thing3 = t3;
    }

    public T1 _1()
    {
        return thing1;
    }

    public T2 _2()
    {
        return thing2;
    }

    public T3 _3()
    {
        return thing3;
    }

    @Override
    public String toString()
    {
        return "Triple [thing1=" + thing1 + ", thing2=" + thing2 + ", thing3=" + thing3 + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((thing1 == null) ? 0 : thing1.hashCode());
        result = prime * result + ((thing2 == null) ? 0 : thing2.hashCode());
        result = prime * result + ((thing3 == null) ? 0 : thing3.hashCode());
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        { return true; }
        if ( obj == null )
        { return false; }
        if ( getClass() != obj.getClass() )
        { return false; }
        Tuple3 other = (Tuple3) obj;
        if ( thing1 == null )
        {
            if ( other.thing1 != null )
            { return false; }
        }
        else if ( !thing1.equals( other.thing1 ) )
        { return false; }
        if ( thing2 == null )
        {
            if ( other.thing2 != null )
            { return false; }
        }
        else if ( !thing2.equals( other.thing2 ) )
        { return false; }
        if ( thing3 == null )
        {
            if ( other.thing3 != null )
            { return false; }
        }
        else if ( !thing3.equals( other.thing3 ) )
        { return false; }
        return true;
    }
}
