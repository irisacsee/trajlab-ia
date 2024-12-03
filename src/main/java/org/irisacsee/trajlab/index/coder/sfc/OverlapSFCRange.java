package org.irisacsee.trajlab.index.coder.sfc;

/**
 * 交叉SFC范围
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class OverlapSFCRange extends SFCRange {
    public OverlapSFCRange(long lower, long upper) {
        super(lower, upper, false);
    }
}
