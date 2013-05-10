package com.facebook.presto.like;

import org.testng.annotations.Test;

import static com.facebook.presto.like.Like.compile;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLike
{
    @Test
    public void testMatchAll()
            throws Exception
    {
        LikeMatcher matcher = compile("%", '\\');

        assertTrue(matcher.matches(""));
        assertTrue(matcher.matches("a"));
        assertTrue(matcher.matches("aa"));
        assertTrue(matcher.matches("aaa"));
        assertTrue(matcher.matches("aaaa"));
        assertTrue(matcher.matches("ab"));
    }

    @Test
    public void testRepeatedWildcard()
            throws Exception
    {
        LikeMatcher matcher = compile("%%%", '\\');

        assertTrue(matcher.matches(""));
        assertTrue(matcher.matches("a"));
        assertTrue(matcher.matches("aa"));
        assertTrue(matcher.matches("aaa"));
        assertTrue(matcher.matches("aaaa"));
        assertTrue(matcher.matches("ab"));
    }

    @Test
    public void testSingleExact()
            throws Exception
    {
        LikeMatcher matcher = compile("a", '\\');

        assertTrue(matcher.matches("a"));

        assertFalse(matcher.matches("b"));
        assertFalse(matcher.matches(""));
    }

    @Test
    public void testSingleAny()
            throws Exception
    {
        LikeMatcher matcher = Like.compile("_", '\\');

        assertTrue(matcher.matches("a"));
        assertTrue(matcher.matches("b"));

        assertFalse(matcher.matches(""));
        assertFalse(matcher.matches("ab"));
    }

    @Test
    public void testMultipleAny()
            throws Exception
    {
        LikeMatcher matcher = Like.compile("___", '\\');

        assertTrue(matcher.matches("aaa"));
        assertTrue(matcher.matches("abc"));

        assertFalse(matcher.matches(""));
        assertFalse(matcher.matches("a"));
        assertFalse(matcher.matches("ab"));
        assertFalse(matcher.matches("abcd"));
    }

    @Test
    public void testPrefix()
            throws Exception
    {
        LikeMatcher matcher = Like.compile("abc%", '\\');

        assertTrue(matcher.matches("abc"));
        assertTrue(matcher.matches("abcd"));
        assertTrue(matcher.matches("abcde"));
    }

    @Test
    public void testSuffix()
            throws Exception
    {
        LikeMatcher matcher = Like.compile("%abc", '\\');

        assertTrue(matcher.matches("abc"));
        assertTrue(matcher.matches("0abc"));
        assertTrue(matcher.matches("01abc"));
    }

    @Test
    public void testContains()
            throws Exception
    {
        LikeMatcher matcher = Like.compile("%abc%", '\\');

        assertTrue(matcher.matches("abc"));
        assertTrue(matcher.matches("0abc"));
        assertTrue(matcher.matches("01abc"));
        assertTrue(matcher.matches("abc0"));
        assertTrue(matcher.matches("abc01"));
        assertTrue(matcher.matches("0abc0"));
        assertTrue(matcher.matches("01abc01"));

        assertFalse(matcher.matches("a"));
        assertFalse(matcher.matches("ab"));
        assertFalse(matcher.matches("xyz"));
        assertFalse(matcher.matches("xyzw"));
    }

    @Test
    public void testEmpty()
            throws Exception
    {
        LikeMatcher matcher = Like.compile("", '\\');

        assertTrue(matcher.matches(""));

        assertFalse(matcher.matches("a"));
        assertFalse(matcher.matches("aa"));
    }

    @Test
    public void testSomething()
            throws Exception
    {
        Like.compile("_", '\\');
    }
}
