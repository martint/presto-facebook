package com.facebook.presto;

import com.google.common.base.Preconditions;
import org.joni.Regex;

public class Slyly3
{
    private final int states = 5;
                                           //  0   1   2   3   4
    // transitions                         //  S   A   B   C   F
    private final int[] transitionCount = {    1,  1,  1,  1,  0  };
    private final int[] elseTransition  = {   -1,  0,  0,  0, -1  };

    private final char[] chars = {
            // S
            'l',
            // A
            'y',
            // B
            'l',
            // C
            'y'
    };

    private final int[] offsets = {
            0,
            1,
            2,
            3,
            4,
            -1
    };

    private final int[] targets = {
            // S
            1, // 'l',
            // A
            2, // 'y',
            // B
            3, // 'l',
            // C
            4, // 'y',
    };

    public boolean matches(byte[] string)
    {
        int currentState = 0;

        for (int i = 0; i < string.length; i++) {
            byte chr = string[i];

            boolean found = false;
            int offset = offsets[currentState];
            int transitions = transitionCount[currentState];
            for (int j = offset; j < offset + transitions; j++) {
                if (chr == chars[j]) {
                    found = true;
                    currentState = targets[j];
                    break;
                }
            }
            if (found) {
                continue;
            }

            if (elseTransition[currentState] != -1) {
                currentState = elseTransition[currentState];
            }
        }

        if (currentState == states - 1) {
            return true;
        }
        return false;
    }

    public static void main(String[] args)
    {
        Slyly3 slyly2 = new Slyly3();

        Preconditions.checkState(slyly2.matches("slyly".getBytes()));
        Preconditions.checkState(slyly2.matches("foo slyly bar".getBytes()));
        Preconditions.checkState(slyly2.matches("foo slyly bar".getBytes()));
        Preconditions.checkState(!slyly2.matches("foo syly bar".getBytes()));

        Slyly3 like = slyly2;
        Regex regex = new Regex(".*slyly.*");

        String value = "ly final dependencies: slyly bold ";

        byte[] valueBytes = value.getBytes();
        int loops = 100_000_000;
        int warmup = 30_000_000;
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
