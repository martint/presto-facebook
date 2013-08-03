package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.dsl.TypeCast;
import com.oracle.truffle.api.dsl.TypeCheck;
import com.oracle.truffle.api.dsl.TypeSystem;

@TypeSystem({Unknown.class, long.class, double.class, boolean.class, String.class})
public class SqlTypes
{
    @TypeCheck
    public boolean isUnknown(Object value)
    {
        return value == null;
    }
//
//    @TypeCast
//    public Unknown asUnknown(Object value)
//    {
//        assert isUnknown(value);
//        return (Unknown) value;
//    }

    @TypeCheck
    public boolean isLong(Object value)
    {
        return value instanceof Long;
    }

    @TypeCast
    public long asLong(Object value)
    {
        assert isLong(value);
        return (long) value;
    }

    @TypeCheck
    public boolean isDouble(Object value)
    {
        return value instanceof Number;
    }

    @TypeCast
    public double asDouble(Object value)
    {
        assert isDouble(value);
        return ((Number) value).doubleValue();
    }
}
