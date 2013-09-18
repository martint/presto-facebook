package com.facebook.presto.sql.newplanner;

import com.facebook.presto.metadata.InMemoryMetadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestNewPlanner
{
    private Analyzer analyzer;
    private MetadataManager metadata;
    private TableHandle t1Handle;

    @Test
    public void testTable()
            throws Exception
    {
        assertEquals(
                plan("TABLE t1"),
                new FunctionCall("table",
                        new TableLiteral(t1Handle),
                        new Tuple(new ColumnLiteral(metadata.getColumnHandle(t1Handle, "a").get()),
                                new ColumnLiteral(metadata.getColumnHandle(t1Handle, "b").get()),
                                new ColumnLiteral(metadata.getColumnHandle(t1Handle, "c").get()),
                                new ColumnLiteral(metadata.getColumnHandle(t1Handle, "d").get()))));

    }

    @Test
    public void testSimpleSelect()
            throws Exception
    {
        assertEquals(
                plan("SELECT * FROM t1"),
                new FunctionCall("table",
                        new TableLiteral(t1Handle),
                        new Tuple(new ColumnLiteral(metadata.getColumnHandle(t1Handle, "a").get()),
                                new ColumnLiteral(metadata.getColumnHandle(t1Handle, "b").get()),
                                new ColumnLiteral(metadata.getColumnHandle(t1Handle, "c").get()),
                                new ColumnLiteral(metadata.getColumnHandle(t1Handle, "d").get()))));
    }


    @Test
    public void testSimpleSelect2()
            throws Exception
    {
        assertEquals(
                plan("SELECT a FROM t1"),
                new FunctionCall("table",
                        new TableLiteral(t1Handle),
                        new Tuple(new ColumnLiteral(metadata.getColumnHandle(t1Handle, "a").get()))));
    }

    @Test
    public void testSimpleProjection()
            throws Exception
    {
        assertEquals(
                plan("SELECT a + b FROM t1"),
                "TO DO"
        );
    }

    @Test
    public void testFilter()
            throws Exception
    {
        assertEquals(
                plan("SELECT * FROM t1 WHERE a > 5"),
                "TO DO"
        );
    }

    @Test
    public void testGroupBy()
            throws Exception
    {
        assertEquals(
                plan("SELECT a, sum(b) FROM t1 GROUP BY a"),
                "TO DO"
        );
    }


    @Test
    public void testGroupBy2()
            throws Exception
    {
        assertEquals(
                plan("SELECT a, sum(a + b) FROM t1 GROUP BY a"),
                "TO DO"
        );
    }

    @Test
    public void testGroupBy3()
            throws Exception
    {
        assertEquals(
                plan("SELECT a, sum(a + b) - avg(b + c) FROM t1 GROUP BY a"),
                "TO DO"
        );
    }

    @Test
    public void testHaving()
            throws Exception
    {
//        assertEquals(
//                plan("SELECT a, sum(b) FROM t1 GROUP BY a HAVING a > 5"),
//                null
//        );

//        assertEquals(
//                plan("SELECT a, sum(b) FROM t1 GROUP BY a HAVING sum(b) > 5"),
//                null
//        );

        assertEquals(
                plan("SELECT a, sum(b) FROM t1 GROUP BY a HAVING avg(c) > 5"),
                null
        );
    }


    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        metadata = new MetadataManager();
        metadata.addConnectorMetadata("tpch", new InMemoryMetadata());

        SchemaTableName table1 = new SchemaTableName("default", "t1");
        t1Handle = metadata.createTable("tpch", new TableMetadata(table1,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", ColumnType.LONG, 0, false),
                        new ColumnMetadata("b", ColumnType.LONG, 1, false),
                        new ColumnMetadata("c", ColumnType.LONG, 2, false),
                        new ColumnMetadata("d", ColumnType.LONG, 3, false))));

        SchemaTableName table2 = new SchemaTableName("default", "t2");
        metadata.createTable("tpch", new TableMetadata(table2,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", ColumnType.LONG, 0, false),
                        new ColumnMetadata("b", ColumnType.LONG, 1, false))));

        SchemaTableName table3 = new SchemaTableName("default", "t3");
        metadata.createTable("tpch", new TableMetadata(table3,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", ColumnType.LONG, 0, false),
                        new ColumnMetadata("b", ColumnType.LONG, 1, false))));

        analyzer = new Analyzer(new Session("user", "test", "tpch", "default", null, null), metadata, Optional.<QueryExplainer>absent());
    }

    private RelationalExpression plan(@Language("SQL") String query)
    {
        Statement statement = SqlParser.createStatement(query);
        Analysis analysis = analyzer.analyze(statement);

        NewPlanner planner = new NewPlanner();
        RelationalExpression plan = planner.plan(analysis);

        System.out.println(plan);

        return plan;
    }

    @Test
    public void testSimple()
            throws Exception
    {
        //        SELECT T.a, T.b, T.c
        //        FROM T
        //
        //        (project
        //                (table 'T')
        //        ((t) -> (t.a t.b t.c)))

//        RelationalExpression expression = new FunctionCall("project",
//                new FunctionCall("table", new StringLiteral("T")),
//                new Lambda("t0", new Tuple(
//                        new FieldRef("t0.a"),
//                        new FieldRef("t0.b"),
//                        new FieldRef("t0.c")
//                )));
    }


//    @Test
//    public void testFilter()
//            throws Exception
//    {
//        //        SELECT T.a, T.b, T.c
//        //        FROM T
//        //        WHERE T.a > 5 AND T.b = 'foo'
//        //
//        //
//        //        (project
//        //                (filter
//        //                        (table 'T')
//        //        ((t) -> (and (> t.a 5) (= t.b 'foo'))))
//        //        ((t) -> (t.a t.b t.c)))
////
////        RelationalExpression expression = new FunctionCall("project",
////                new FunctionCall("filter",
////                        new FunctionCall("table", new StringLiteral("T")),
////                        new Lambda("t0", new BinaryLogicalExpression("and",
////                                new ComparisonExpression(">", new Reference("t0.a"), new StringLiteral(5)),
////                                new ComparisonExpression("=", new Reference("t0.b"), new StringLiteral("foo"))))),
////                new Lambda("t0", new Tuple(
////                        new Reference("t0.a"),
////                        new Reference("t0.b"),
////                        new Reference("t0.c")
////                )));
////
//
//    }

    @Test
    public void testJoin()
            throws Exception
    {
        //        SELECT T.a, T.b, U.x, U.y
        //        FROM T JOIN U ON T.a = U.x
        //
        //        (project
        //          (join
        //              (table 'T')
        //              (table 'U')
        //              ((left, right) -> (= left.a left.x)))
        //          ((m0) -> (m0.t.a m0.t.b m0.u.x m0.u.y)))

//
//        RelationalExpression expression = new FunctionCall("project",
//                new FunctionCall("join",
//                        new FunctionCall("table", new StringLiteral("T")),
//                        new FunctionCall("table", new StringLiteral("U")),
//                        new Lambda("left0", "right0", new ComparisonExpression("=", new Reference("left0.a"), new Reference("right0.x")))),
//                new Lambda("m0", new Tuple(
//                        new Reference("m0.t.a"),
//                        new Reference("m0.t.b"),
//                        new Reference("m0.u.x"),
//                        new Reference("m0.u.y")
//                )));
    }


    @Test
    public void test3WayJoin()
            throws Exception
    {
        //        SELECT T.a, T.b, U.x, U.y, V.z
        //        FROM T JOIN U ON T.a = U.x
        //               JOIN V ON T.a = V.z
        //
        //        (project
        //          (join
        //              (join
        //                  (table 'T')
        //                  (table 'U')
        //                  ((m0) -> (= m0.t.a m0.u.x))))
        //              (table 'V')
        //              ((m0) -> (= m0.t.a m0.u.x))))
        //          ((m0) -> (m.t.a m.t.b m.u.x m.u.y)))

//        RelationalExpression expression = new FunctionCall("project",
//                new FunctionCall("join",
//                        new FunctionCall("join",
//                                new FunctionCall("table", new StringLiteral("T")),
//                                new FunctionCall("table", new StringLiteral("U")),
//                                new Lambda("left0", "right0", new ComparisonExpression("=", new Reference("left0.a"), new Reference("right0.x")))),
//                        new FunctionCall("table", new StringLiteral("V")),
//                        new Lambda("left0", "right0", new ComparisonExpression("=", new Reference("left0.????"), new Reference("right0.???")))),
//                new Lambda("m0", new Tuple(
//                        new Reference("m0.t.a"),
//                        new Reference("m0.t.b"),
//                        new Reference("m0.u.x"),
//                        new Reference("m0.u.y")
//                )));

    }
}
