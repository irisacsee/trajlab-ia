package org.irisacsee.trajlab.index.coder.sfc;

/**
 * 覆盖SFC范围
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class CoveredSFCRange extends SFCRange {
    public CoveredSFCRange(long lower, long upper) {
        super(lower, upper, true);
    }
}
