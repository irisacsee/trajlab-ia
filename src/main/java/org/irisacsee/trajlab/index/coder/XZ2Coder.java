package org.irisacsee.trajlab.index.coder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.index.coder.sfc.SFCRange;
import org.irisacsee.trajlab.index.coder.sfc.XZ2SFC;
import org.irisacsee.trajlab.query.condition.SpatialQueryCondition;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * XZ2编码器
 *
 * @author irisacsee
 * @since 2024/11/20
 */
@Slf4j
public class XZ2Coder extends AbstractCoder<SpatialQueryCondition> {
    public static final int BYTES = Long.BYTES;
    @Getter
    private final XZ2SFC sfc;
    private final short xz2Precision;

    public XZ2Coder() {
        xz2Precision = CodeConstant.MAX_XZ2_PRECISION;
        sfc = XZ2SFC.getInstance(xz2Precision);
    }

    /**
     * Get xz2 index for the line string.
     *
     * @param lineString Line string to be indexed.
     * @return The XZ2 code
     */
    public byte[] code(LineString lineString) {
        Envelope boundingBox = lineString.getEnvelopeInternal();
        double minLng = boundingBox.getMinX();
        double maxLng = boundingBox.getMaxX();
        double minLat = boundingBox.getMinY();
        double maxLat = boundingBox.getMaxY();
        // lenient is false so the points out of boundary can throw exception.
        ByteBuffer br = ByteBuffer.allocate(Long.BYTES);
        br.putLong(sfc.index(minLng, maxLng, minLat, maxLat, false));
        return br.array();
    }

    /**
     * Get index ranges of the query range, support two spatial query types
     *
     * @param spatialQueryCondition Spatial query on the index.
     * @return List of xz2 index ranges corresponding to the query range.
     */
    @Override
    public List<CodeRange> ranges(SpatialQueryCondition spatialQueryCondition) {
        Envelope envelope = spatialQueryCondition.getQueryWindow();
        List<SFCRange> sfcRangeList = sfc.ranges(envelope,
                spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN);
        List<CodeRange> codeRangeList = new ArrayList<>(sfcRangeList.size());
        for (SFCRange sfcRange : sfcRangeList) {
            CodeRange codeRange = new CodeRange();
            codeRange.concatSfcRange(sfcRange);
            codeRangeList.add(codeRange);
        }
        return codeRangeList;
    }

    @Override
    public List<CodeRange> rangesWithPartition(SpatialQueryCondition spatialQueryCondition) {
        Envelope envelope = spatialQueryCondition.getQueryWindow();
        List<SFCRange> sfcRangeList = sfc.ranges(envelope,
                spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN);
        List<SFCRange> sfcRanges = splitPartitionSFCRange(sfcRangeList);
        List<CodeRange> codeRangeList = new ArrayList<>(sfcRangeList.size());
        for (SFCRange sfcRange : sfcRanges) {
            CodeRange codeRange = new CodeRange();
            codeRange.concatSfcRange(sfcRange);
            codeRangeList.add(codeRange);
        }
        return codeRangeList;
    }

    public Polygon getCodingPolygon(ByteArray spatialCodingByteArray) {
        ByteBuffer br = spatialCodingByteArray.toByteBuffer();
        br.flip();
        long coding = br.getLong();
        return sfc.getEnlargedRegion(coding);
    }

    @Override
    public String toString() {
        return "XZ2Coding{" + "xz2Precision=" + xz2Precision + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XZ2Coder xz2Coding = (XZ2Coder) o;
        return xz2Precision == xz2Coding.xz2Precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xz2Precision);
    }
}
