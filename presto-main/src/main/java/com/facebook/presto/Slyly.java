package com.facebook.presto;

import com.google.common.base.Preconditions;
import org.joni.Option;
import org.joni.Regex;

import java.util.Arrays;

public class Slyly
{
    private final int states = 6;

    // transitions
    private final boolean[] hasConcreteChar = {true,   true,  true,  true,  true,  true };
    private final char[] chars =              {'s',    'l',   'y',   'l',   'y',   0    };
    private final boolean[] hasWildcard =     {true,   false, false, false, false, true };

    // do the states match a wildcard
    private final int[] wildcardTarget =      { 0,     -1,     -1,     -1,     -1,    5    };

    private boolean[] activeStates =          { false, false, false, false, false, false};

    public boolean matches(String string)
    {
        //        Arrays.fill(activeStates, false);
        activeStates[activeStates.length - 1]  = false;

        activeStates[0] = true;
        int maxActiveState = 0;

        for (int i = 0; i < string.length(); i++) {
            char chr = string.charAt(i);

            boolean anyActive = false;
            for (int state = maxActiveState; state >= 0; --state) {
                if (activeStates[state]) {
                    activeStates[state] = false;

                    if (chr == chars[state]) {
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
        Preconditions.checkState(new Slyly().matches("slyly"));
        Preconditions.checkState(new Slyly().matches("foo slyly bar"));
        Preconditions.checkState(new Slyly().matches("foo slyly bar"));
        Preconditions.checkState(!new Slyly().matches("foo syly bar"));

        Slyly like = new Slyly();
        Regex regex = new Regex(".*slyly.*");

        String value = "ly final dependencies: slyly bold ";

        byte[] valueBytes = value.getBytes();
        int loops = 30_000_000;
        int warmup = 10_000_000;
        long start = System.nanoTime();
        for (int i = 0; i < loops; i++) {
            if (i == warmup) {
                start = System.nanoTime();
            }
//            regex.matcher(valueBytes).match(0, value.length(), Option.NONE);
            like.matches(value);
        }
        long end = System.nanoTime();
        System.out.println((end - start) / (loops - warmup));
        System.out.println((loops - warmup) * 1_000_000_000L / (end - start));


    }
}
