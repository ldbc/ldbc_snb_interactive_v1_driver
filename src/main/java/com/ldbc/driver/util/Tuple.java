package com.ldbc.driver.util;

public class Tuple
{
    public static <Type1, Type2> Tuple2<Type1,Type2> tuple2( Type1 t1, Type2 t2 )
    {
        return new Tuple2<>( t1, t2 );
    }

    public static <Type1, Type2, Type3> Tuple3<Type1,Type2,Type3> tuple3( Type1 t1, Type2 t2, Type3 t3 )
    {
        return new Tuple3<>( t1, t2, t3 );
    }
}
