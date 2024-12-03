package org.irisacsee.trajlab.index.coder;

import lombok.extern.slf4j.Slf4j;
import org.irisacsee.trajlab.index.coder.sfc.SFCRange;
import org.irisacsee.trajlab.index.coder.sfc.XZTSFC;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.index.type.TimeBin;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.index.type.TimePeriod;
import org.irisacsee.trajlab.query.condition.TemporalQueryCondition;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class XZTCoder extends AbstractCoder<TemporalQueryCondition> {
    private static final ZonedDateTime EPOCH = ZonedDateTime.ofInstant(Instant.EPOCH, CodeConstant.TIME_ZONE);
    public static final int BYTES = Long.BYTES;
    private final int g;
    private final TimePeriod timePeriod;
    private final XZTSFC sfc;

    public XZTCoder() {
        g = CodeConstant.MAX_TIME_BIN_PRECISION;
        timePeriod = CodeConstant.DEFAULT_TIME_PERIOD;
        sfc = XZTSFC.apply(g, timePeriod);
    }

    public long value(ZonedDateTime start, ZonedDateTime end) {
        TimeBin bin = dateToBinnedTime(start);
        return sfc.index(start, end, bin);
    }

    public long value(TimeLine timeLine) {
        return value(timeLine.getTimeStart(), timeLine.getTimeEnd());
    }

    public byte[] code(ZonedDateTime start, ZonedDateTime end) {
        ByteBuffer br = ByteBuffer.allocate(BYTES);
        long index = value(start, end);
        br.putLong(index);
        return br.array();
    }

    public byte[] code(TimeLine timeLine) {
        return code(timeLine.getTimeStart(), timeLine.getTimeEnd());
    }

    public long getElementCode(long xztValue) {
        return xztValue % sfc.binElementCnt();
    }

    public TimeBin getTimeBin(long xztValue) {
        return sfc.getTimeBin(xztValue);
    }

    @Override
    public List<CodeRange> ranges(TemporalQueryCondition condition) {
        return timeIndexRangesToCodeRanges(sfc.ranges(condition.getQueryWindows(),
                condition.getQueryType() == TemporalQueryCondition.TemporalQueryType.CONTAIN));
    }

    @Override
    public List<CodeRange> rangesWithPartition(TemporalQueryCondition condition) {
        return timeIndexRangesToCodeRanges(splitPartitionSFCRange(sfc.ranges(condition.getQueryWindows(),
                condition.getQueryType() == TemporalQueryCondition.TemporalQueryType.CONTAIN)));
    }

    public TimeBin dateToBinnedTime(ZonedDateTime zonedDateTime) {
        int binId = (int) timePeriod.getChronoUnit().between(EPOCH, zonedDateTime);
        return new TimeBin(binId, timePeriod);
    }

    public List<CodeRange> timeIndexRangesToCodeRanges(List<SFCRange> timeIndexRanges) {
        List<CodeRange> codeRanges = new ArrayList<>(timeIndexRanges.size());
        for (SFCRange sr : timeIndexRanges) {
            CodeRange codingRange = new CodeRange();
            codingRange.concatTimeIndexRange(sr);
            codeRanges.add(codingRange);
        }
        return codeRanges;
    }

    /**
     * @param coding Time coding
     * @return Sequences 0 and 1 of time
     */
    public List<Integer> getSequenceCode(long coding) {
        int g = this.g;
        // coding 减去 bin cnt
        coding = getElementCode(coding);
        List<Integer> list = new ArrayList<>(g);
        for (int i = 0; i < g; i++) {
            if (coding <= 0) {
                break;
            }
            long operator = (long) Math.pow(2, g - i) - 1;
            long s = ((coding - 1) / operator);
            list.add((int) s);
            coding = coding - 1L - s * operator;
        }
        return list;
    }

    /**
     * Obtaining Minimum Time Bounding Box Based on Coding Information
     *
     * @param coding  Time coding
     * @return With time starting point and end point information
     */
    public TimeLine getXZTElementTimeLine(long coding) {
        TimeBin timeBin = getTimeBin(coding);
        List<Integer> list = getSequenceCode(coding);
        double timeMin = 0.0;
        double timeMax = 1.0;
        for (Integer integer : list) {
            double timeCenter = (timeMin + timeMax) / 2;
            if (integer == 0) {
                timeMax = timeCenter;
            } else {
                timeMin = timeCenter;
            }
        }
        ZonedDateTime binStartTime = timeBinToDate(timeBin);
        long timeStart = (long) (timeMin * timePeriod.getChronoUnit().getDuration().getSeconds())
                + binStartTime.toEpochSecond();
        long timeEnd = (long) ((timeMax * timePeriod.getChronoUnit().getDuration().getSeconds())
                + binStartTime.toEpochSecond());
        ZonedDateTime startTime = timeToZonedTime(timeStart);
        ZonedDateTime endTime = timeToZonedTime(timeEnd);

        return new TimeLine(startTime, endTime);
    }

    public static ZonedDateTime timeBinToDate(TimeBin binnedTime) {
        long bin = binnedTime.getBid();
        return binnedTime.getTimePeriod().getChronoUnit().addTo(EPOCH, bin);
    }

    public static ZonedDateTime timeToZonedTime(long time) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), CodeConstant.TIME_ZONE);
    }
}
