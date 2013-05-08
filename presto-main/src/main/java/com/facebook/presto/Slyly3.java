package com.facebook.presto;

import com.google.common.base.Preconditions;
import org.joni.Regex;

public class Slyly3
{
    private final int states = 6;
                                           //   0       1       2       3        4      5
    // transitions                         // (0,1)  (0,1,2) (0,1,3)  (0,1,4) (0,1,5)  (f)
    private final int[] transitionCount = {    1,       2,       2,      2,       2,   0     };
    private final int[] elseTransition  = {    0,       0,       0,      0,       0,   -1    };

    private final char[] chars = {
            // (0,1)
            's',
            // (0,1,2)
            's',
            'l',
            // (0,1,3)
            's',
            'y',
            // (0,1,4)
            's',
            'l',
            // (0,1,5)
            's',
            'y'};

    private final int[] offsets = {
            0,
            1,
            3,
            5,
            7,
            -1
    };

    private final int[] targets = {
            // (0,1)
            1, // 's',
            // (0,1,2)
            1, // 's',
            2, // 'l',
            // (0,1,3)
            1, // 's',
            3, // 'y',
            // (0,1,4)
            1, // 's',
            4, // 'l',
            // (0,1, 5)
            1, // 's',
            5, //'y'
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
