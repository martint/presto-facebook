package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.nodes.Node;

@TypeSystemReference(SqlTypes.class)
public class SqlNode extends Node
{
}
