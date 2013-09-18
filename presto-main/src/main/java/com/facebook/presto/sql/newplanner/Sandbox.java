package com.facebook.presto.sql.newplanner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.DoubleAccumulator;

public class Sandbox
{
    public static RelationalExpression call(String name, RelationalExpression... args)
    {
        return new FunctionCall(name, args);
    }

    public static RelationalExpression tableRef(final String name)
    {
        return new TableLiteral(new TableHandle()
        {
            @Override
            public String toString()
            {
                return name;
            }
        });
    }

    public static RelationalExpression colRef(final String name)
    {
        return new ColumnLiteral(new ColumnHandle()
        {
            @Override
            public String toString()
            {
                return name;
            }
        });
    }

    public static RelationalExpression tuple(RelationalExpression... args)
    {
        return new Tuple(args);
    }

    public static RelationalExpression string(String value)
    {
        return new StringLiteral(value);
    }

    public static RelationalExpression field(RelationalExpression expression, int field)
    {
        return new FieldRef(expression, field);
    }

    public static void main(String[] args)
            throws Exception
    {
        call("aggregate",
                call("table", tableRef("default.t1"), tuple(colRef("tpch:a:0"), colRef("tpch:b:1"), colRef("tpch:c:2"), colRef("tpch:d:3"))),
                t -> tuple(field(t, 0)),
                tuple(string("sum"), t -> tuple(call("ADD", field(t, 0), field(t, 1)))),
                tuple(string("avg"), t -> tuple(call("ADD", field(t, 1), field(t, 2)))));
        //        call("project",
        //                call("group",
        //                        call("table", tableRef("default.t1"), tuple(colRef("tpch:a:0"), colRef("tpch:b:1"), colRef("tpch:c:2"), colRef("tpch:d:3"))),
        //                        t -> tuple(field(t, 0))),
        //                t -> tuple(field(field(t, 0), 0), call("sum", field(field(t, 1), 1))));

        //
        //        call("project",
        //                call("group",
        //                        call("table", tableRef("default.t1"), tuple(colRef("tpch:a:0"), colRef("tpch:b:1"), colRef("tpch:c:2"), colRef("tpch:d:3"))),
        //                        t -> tuple(field(t, 0))),
        //                t -> tuple(field(field(t, 0), 0), call("sum", call("+", field(field(t, 1), 0), field(field(t, 1), 1)))));
        //
    }

}

