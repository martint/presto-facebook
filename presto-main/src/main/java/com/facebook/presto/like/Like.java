package com.facebook.presto.like;

import com.google.common.base.Preconditions;

public class Like
{
    // transitions
    private final boolean[] hasConcreteChar = {true,   true,  false, true,  true,  true,  true,  false};
    private final char[] chars =              { 'a',   'b',   0,     'a',   'b',   'c',   'd',   0};
    private final boolean[] hasWildcard =     {true,   false, true,  false, false, true, false, true};

    // do the states match a wildcard
    private final int[] wildcardTarget =      { 0,     -1,    3,     -1,    -1,    5,    -1,    7};


    private boolean[] activeStates =          { false, false, false, false, false, false, false, false};

    public boolean matches(String string)
    {
        activeStates[0] = true;
        int maxActiveState = 0;

        for (int i = 0; i < string.length(); i++) {
            char chr = string.charAt(i);

            boolean anyActive = false;
            for (int state = maxActiveState; state >= 0; --state) {
                if (activeStates[state]) {
                    activeStates[state] = false;

                    if (hasConcreteChar[state] && chr == chars[state]) {
                        int target = state + 1;

                        activeStates[target] = true;
                        maxActiveState = Math.max(maxActiveState, target);
                        anyActive = true;
                    }

                    if (hasWildcard[state]) {
                        int target = wildcardTarget[state];

                        activeStates[target] = true;
                        maxActiveState = Math.max(maxActiveState, target);
                        anyActive = true;
                    }
                }
            }

            if (!anyActive) {
                return false;
            }

            if (activeStates[activeStates.length - 1] && hasWildcard[activeStates.length - 1]) {
                return true;
            }

        }

        if (activeStates[activeStates.length - 1]) {
            return true;
        }

        return false;
    }

    public static void main(String[] args)
    {

        Preconditions.checkState(new Like().matches("abcabcd"));
        Preconditions.checkState(new Like().matches("xxabcabcdyyy"));
        Preconditions.checkState(new Like().matches("xxabeabcdyyy"));
        Preconditions.checkState(new Like().matches("xxabeabxxcdyyy"));

        Preconditions.checkState(!new Like().matches("foo"));

    }
}
