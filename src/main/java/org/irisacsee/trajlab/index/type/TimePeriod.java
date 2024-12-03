package org.irisacsee.trajlab.index.type;

import java.time.temporal.ChronoUnit;

/**
 * 时间间隔枚举
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public enum TimePeriod {
    DAY(ChronoUnit.DAYS),
    WEEK(ChronoUnit.WEEKS),
    MONTH(ChronoUnit.MONTHS),
    YEAR(ChronoUnit.YEARS);

    ChronoUnit chronoUnit;

    TimePeriod(ChronoUnit chronoUnit) {
        this.chronoUnit = chronoUnit;
    }

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    @Override
    public String toString() {
        return "TimePeriod{" +
                "chronoUnit=" + chronoUnit +
                '}';
    }
}
