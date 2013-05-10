package com.facebook.presto.like;

import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkArgument;

public class LikeMatcher
{
    private final int numberOfStates;

    // one entry per state
    private final int[] numberOfTransitions;
    private final int[] elseTargets;
    private final int[] offsets;

    // one entry per state + symbol
    // the symbols and targets for a given state start at offsets[state] and end at offsets[state] +  numberOfTransitions[state] - 1
    private final char[] symbols;
    private final int[] targets;

    private final int acceptingState;
    private final int startingState;

    public LikeMatcher(int numberOfStates, int startingState, int acceptingState, int[] numberOfTransitions, int[] elseTargets, int[] offsets, char[] symbols, int[] targets)
    {
        Preconditions.checkNotNull(numberOfTransitions, "numberOfTransitions is null");
        Preconditions.checkNotNull(elseTargets, "elseTransitions is null");
        Preconditions.checkNotNull(offsets, "offsets is null");
        Preconditions.checkNotNull(symbols, "symbols is null");
        Preconditions.checkNotNull(targets, "targets is null");

        this.numberOfStates = numberOfStates;
        this.numberOfTransitions = numberOfTransitions;
        this.elseTargets = elseTargets;
        this.offsets = offsets;
        this.symbols = symbols;
        this.targets = targets;
        this.acceptingState = acceptingState;
        this.startingState = startingState;

        validate();
    }

    private void validate()
    {
        checkArgument(numberOfStates == numberOfTransitions.length, "size of numberOfTransitions (%s) doesn't match number of states (%s)", numberOfTransitions.length, numberOfStates);
        checkArgument(numberOfStates == elseTargets.length, "size of elseTransitions (%s) doesn't match number of states (%s)", elseTargets.length, numberOfStates);
        checkArgument(numberOfStates == offsets.length, "size of offsets (%s) doesn't match number of stats (%s)", elseTargets.length, numberOfStates);
        checkArgument(startingState >= 0 && startingState < numberOfStates, "startingState (%s) must be a valid state (0..%s)", startingState, numberOfStates - 1);
        checkArgument(acceptingState >= 0 && acceptingState < numberOfStates, "acceptingState (%s) must be a valid state (0..%s)", acceptingState, numberOfStates - 1);
        checkArgument(symbols.length == targets.length, "symbols size (%) doesn't match targets size (%)", symbols.length, targets.length);

        for (int i = 0; i < numberOfStates; i++) {
            checkArgument(offsets[i] >= 0 && offsets[i] <= symbols.length, "offset (%s) for state %s must be >= 0 and < %s", offsets[i], i, symbols.length);
            checkArgument(numberOfTransitions[i] >= 0 && numberOfTransitions[i] <= numberOfStates, "numberOfTransitions (%s) for state %s must be between 0 and %s", numberOfTransitions[i], i, numberOfStates);
            checkArgument(elseTargets[i] >= 0 && elseTargets[i] < numberOfStates, "elseTarget (%s) for state (%s) must be a valid state (0..%s)", elseTargets[i], i, numberOfStates - 1);
        }

        // validate that all targets are valid states
        for (int i = 0; i < targets.length; i++) {
            checkArgument(targets[i] >= 0 && targets[i] < numberOfStates, "target (%s) at position %s (symbol '%s') must be a valid state (0..%s)", targets[i], i, symbols[i], numberOfStates - 1);
        }

        int totalTransitions = 0;
        for (int count : numberOfTransitions) {
            totalTransitions += count;
        }

        checkArgument(totalTransitions <= symbols.length, "sum of numberOfTransitions (%s) must be >= 0 and < %s", totalTransitions, symbols.length);

        // TODO: validate non-overlapping regions in symbols/targets
    }

    public boolean matches(String string)
    {
        int currentState = startingState;

        for (int i = 0; i < string.length(); i++) {
            int transitions = numberOfTransitions[currentState];
            int elseTransition = elseTargets[currentState];

            if (transitions == 0 && elseTransition == currentState) {
                // if this is a state we can't get out of, we're done
                break;
            }

            char character = string.charAt(i);
            int offset = offsets[currentState];
            boolean found = false;
            for (int j = offset; j < offset + transitions; j++) {
                if (character == symbols[j]) {
                    found = true;
                    currentState = targets[j];
                    break;
                }
            }
            if (found) {
                continue;
            }

            currentState = elseTransition;
        }

        return currentState == acceptingState;
    }

}
