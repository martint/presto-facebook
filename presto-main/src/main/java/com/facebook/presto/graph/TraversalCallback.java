package com.facebook.presto.graph;

public interface TraversalCallback<T>
{
    void before(T node, int discoveryTime);
    void after(T node, int discoveryTime, int visitTime);
    void cycleDetected(T node, T neighbor);
}
