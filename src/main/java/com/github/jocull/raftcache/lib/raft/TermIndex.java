package com.github.jocull.raftcache.lib.raft;

import java.util.Objects;

public class TermIndex implements Comparable<TermIndex> {
    private final int term;
    private final long index;

    public TermIndex(int term, long index) {
        if (term < 0) {
            throw new IllegalArgumentException("Term must be >= 0");
        }
        if (index < 0) {
            throw new IllegalArgumentException("Index must be >= 0");
        }

        this.term = term;
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    /**
     * A log is directly before another if it has a term <= and index -= 1 to the other
     */
    public boolean isDirectlyBefore(TermIndex other) {
        return term <= other.term
                && index == (other.index - 1);
    }

    /**
     * A log is directly after another if it has a term >= and index += 1 to the other
     */
    public boolean isDirectlyAfter(TermIndex other) {
        return term >= other.term
                && index == (other.index + 1);
    }

    /**
     * Replacement occurs when a log presents a higher term for a lower index.
     * It means that the log it is matched against should be discarded (and all of those following it).
     */
    public boolean isReplacementOf(TermIndex other) {
        return term > other.term
                && index <= other.index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermIndex termIndex = (TermIndex) o;
        return term == termIndex.term && index == termIndex.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index);
    }

    @Override
    public String toString() {
        return "TermIndex{" +
                "term=" + term +
                ", index=" + index +
                '}';
    }

    @Override
    public int compareTo(TermIndex other) {
        Objects.requireNonNull(other, "TermIndex cannot be compared to null");
        final int termCompare = Integer.compare(term, other.term);
        if (termCompare != 0) {
            return termCompare;
        }
        return Long.compare(index, other.index);
    }
}
