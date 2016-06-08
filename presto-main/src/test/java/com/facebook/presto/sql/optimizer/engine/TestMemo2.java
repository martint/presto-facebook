package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Filter;
import com.facebook.presto.sql.optimizer.tree.Get;
import com.facebook.presto.sql.optimizer.tree.Project;
import org.testng.annotations.Test;

public class TestMemo2
{
    @Test//(expectedExceptions = IllegalStateException.class)
    public void testCannotMergeParentWithChild()
            throws Exception
    {
        Memo2 memo = new Memo2();
        String group = memo.insert(new Filter("a", new Project("b", new Get("t"))));
        memo.insert(group, new Project("b", new Get("t")));
    }
}
