package com.facebook.presto.like;

import com.google.common.base.Preconditions;
import org.joni.Regex;

public class Slyly2
{
    private final int states = 6;

    // transitions
    private final byte[] chars =     { 's',   'l',   'y',   'l',   'y',   0 };
    private final boolean[] isNone = { true,  false, false, false, false, true };
    private final boolean[] isAny =  { false, false, false, false, false, false };


    private int[] currentStates = new int[states];
    private int[] newStates = new int[states];

    public boolean matches(byte[] string)
    {
        currentStates[0] = 0;
        int maxCurrentState = 0;
        int maxNewState = -1;

        for (int i = 0; i < string.length; i++) {
            byte chr = string[i];

            for (int j = 0; j <= maxCurrentState; ++j) {
                int state = currentStates[j];

                if (isNone[state]) {
                    newStates[++maxNewState] = state;
                }

                if (chr == chars[state] || isAny[state]) {
                    newStates[++maxNewState] = state + 1;
                }
            }

            // no valid transitions from any current state, so the string doesn't match
            if (maxNewState == -1) {
                return false;
            }

            // if the last state is active and has a an any transition to itself (i.e., a '%')
            // then there's not point in checking the rest of the string
            if (newStates[maxNewState] == states - 1 && isNone[states - 1]) {
                return true;
            }

            // max new -> current (and reuse current instead of creating a new array)
            int[] tmp = currentStates;
            currentStates = newStates;
            newStates = tmp;

            // and reset the pointers
            maxCurrentState = maxNewState;
            maxNewState = -1;
        }

        // if the last state is active, we have a match
        if (currentStates[maxCurrentState] == states - 1) {
            return true;
        }

        return false;
    }

    public static void main(String[] args)
    {
        Slyly2 slyly2 = new Slyly2();

        Preconditions.checkState(slyly2.matches("slyly".getBytes()));
        Preconditions.checkState(slyly2.matches("foo slyly bar".getBytes()));
        Preconditions.checkState(slyly2.matches("foo slyly bar".getBytes()));
        Preconditions.checkState(!slyly2.matches("foo syly bar".getBytes()));

        Slyly2 like = slyly2;
        Regex regex = new Regex(".*slyly.*");

        String value = "ly final dependencies: slyly bold ";

        byte[] valueBytes = value.getBytes();
        int loops = 50_000_000;
        int warmup = 20_000_000;
        long start = System.nanoTime();
        for (int i = 0; i < loops; i++) {
            if (i == warmup) {
                start = System.nanoTime();
            }
            //            regex.matcher(valueBytes).match(0, value.length(), Option.NONE);
            like.matches(valueBytes);
        }
        long end = System.nanoTime();
        System.out.println((end - start) / (loops - warmup));
        System.out.println((loops - warmup) * 1_000_000_000L / (end - start));


    }
}
