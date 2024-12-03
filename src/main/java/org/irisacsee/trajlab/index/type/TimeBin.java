package org.irisacsee.trajlab.index.type;

import org.irisacsee.trajlab.constant.CodeConstant;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;


/**
 * 时间段
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class TimeBin {
  /** 将连续时间分割为多个紧密相连的bucket, 单位为 day, week, month, year */
  private final int bid;

  private final TimePeriod timePeriod;

  @SuppressWarnings("checkstyle:StaticVariableName")
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, CodeConstant.TIME_ZONE);

  public TimeBin(int bid, TimePeriod timePeriod) {
    this.bid = bid;
    this.timePeriod = timePeriod;
  }

  public int getBid() {
    return bid;
  }

  public TimePeriod getTimePeriod() {
    return timePeriod;
  }

  /**
   * @return min date time of the bin (inclusive)
   */
  public ZonedDateTime getBinStartTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, bid);
  }

  /**
   * @return max date time of the bin(exculsive)
   */
  public ZonedDateTime getBinEndTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, bid + 1);
  }

  /**
   * 获取refTime相对当前Bin起始时间的相对值（秒）
   *
   * @param time 任意时间
   * @return time相对当前Bin起始时间的秒数
   */
  public long getRefTime(ZonedDateTime time) {
    return time.toEpochSecond() - getBinStartTime().toEpochSecond();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeBin timeBin = (TimeBin) o;
    return bid == timeBin.bid && timePeriod == timeBin.timePeriod;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bid, timePeriod);
  }

  @Override
  public String toString() {
    return "TimeBin{" + "start=" + getBinStartTime() + ", timePeriod=" + timePeriod + '}';
  }
}
