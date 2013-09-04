package com.facebook.presto.sql.newplanner;

import com.facebook.presto.sql.analyzer.Field;

import java.util.HashMap;
import java.util.Map;

public class PlanningContext
{
    private final Map<Integer, Integer> fieldToIndex = new HashMap<>();

    public String getCurrentTupleReference()
    {
        // TODO
        return "t";
    }

    public void setMapping(Field field, int index)
    {
        // TODO: ?
        fieldToIndex.put(index, index);
    }

    // TODO: translation table of fieldOrExpression -> tuple offset
    // TODO: translation table
}
