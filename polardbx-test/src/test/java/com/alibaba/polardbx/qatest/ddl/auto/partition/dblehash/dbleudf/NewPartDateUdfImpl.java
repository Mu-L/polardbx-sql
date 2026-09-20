/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.RuleAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.StringUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class NewPartDateUdfImpl extends UserDefinedJavaFunction {
    private DblePartitionAlgorithm routeFunc;

    public NewPartDateUdfImpl(int defaultNode,
                              String dateFormat,
                              String sBeginDate,
                              String sEndDate,
                              String sPartitionDay) {
        init(defaultNode, dateFormat, sBeginDate, sEndDate, sPartitionDay);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }

    private void init(int defaultNode,
                      String dateFormat,
                      String sBeginDate,
                      String sEndDate,
                      String sPartitionDay) {
        NewPartitionByDate newPartitionByDate = new NewPartitionByDate();
        newPartitionByDate.setDefaultNode(defaultNode);
        newPartitionByDate.setDateFormat(dateFormat);
        newPartitionByDate.setsBeginDate(sBeginDate);
        newPartitionByDate.setsEndDate(sEndDate);
        newPartitionByDate.setsPartionDay(sPartitionDay);
        newPartitionByDate.init();
        routeFunc = newPartitionByDate;
    }

    public class NewPartitionByDate extends DblePartitionAlgorithm implements RuleAlgorithm {
        private static final long serialVersionUID = 4966421543458534122L;

        private String sBeginDate;
        private String sEndDate;
        private String sPartionDay;
        private String dateFormat;

        private long beginDate;
        private long partitionTime;
        private long endDate;
        private int nCount;
        private int defaultNode = -1;
        private SimpleDateFormat formatter;
        private static final long ONE_DAY = 86400000;
        private int hashCode = -1;

        public NewPartitionByDate() {
        }

        @Override
        public void init() {
            try {
                partitionTime = Integer.parseInt(sPartionDay) * ONE_DAY;

                beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

                if (!StringUtil.isEmpty(sEndDate)) {
                    endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                    nCount = (int) ((endDate - beginDate) / partitionTime) + 1;
                }
                formatter = new SimpleDateFormat(dateFormat);
            } catch (ParseException e) {
                throw new IllegalArgumentException(e);
            }

            initHashCode();
        }


        @Override
        public void selfCheck() {
            StringBuffer sb = new StringBuffer();

            if (sBeginDate == null || "".equals(sBeginDate)) {
                sb.append("sBeginDate can not be null\n");
            } else {
                try {
                    new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();
                } catch (Exception e) {
                    sb.append("pause beginDate error\n");
                }
            }

            if (dateFormat == null || "".equals(dateFormat)) {
                sb.append("dateFormat can not be null\n");
            } else {
                if (!StringUtil.isEmpty(sEndDate)) {
                    try {
                        new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                    } catch (Exception e) {
                        sb.append("pause endDate error\n");
                    }
                }
            }

            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
                throw new RuntimeException(sb.toString());
            }
        }

        @Override
        public Integer calculate(String columnValue) {
            try {
                if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
                    if (defaultNode >= 0) {
                        return defaultNode;
                    }
                    return null;
                }
                long targetTime = formatter.parse(columnValue).getTime();
                if (targetTime < beginDate) {
                    return (defaultNode >= 0) ? defaultNode : null;
                }
                int targetPartition = (int) ((targetTime - beginDate) / partitionTime);

                if (targetTime > endDate && nCount != 0) {
                    targetPartition = targetPartition % nCount;
                }
                return targetPartition;

            } catch (ParseException e) {
                throw new IllegalArgumentException("columnValue:" + columnValue + " Please check if the format satisfied.", e);
            }
        }

        @Override
        public Integer[] calculateRange(String beginValue, String endValue) {
            SimpleDateFormat format = new SimpleDateFormat(this.dateFormat);
            try {
                Date begin = format.parse(beginValue);
                Date end = format.parse(endValue);
                Calendar cal = Calendar.getInstance();
                List<Integer> list = new ArrayList<>();
                while (begin.getTime() <= end.getTime()) {
                    Integer nodeValue = this.calculate(format.format(begin));
                    if (Collections.frequency(list, nodeValue) < 1) list.add(nodeValue);
                    cal.setTime(begin);
                    cal.add(Calendar.DATE, 1);
                    begin = cal.getTime();
                }

                Integer[] nodeArray = new Integer[list.size()];
                for (int i = 0; i < list.size(); i++) {
                    nodeArray[i] = list.get(i);
                }

                return nodeArray;
            } catch (ParseException e) {
                return new Integer[0];
            }
        }

        @Override
        public int getPartitionNum() {
            int count = this.nCount;
            return count > 0 ? count : -1;
        }

        public void setsBeginDate(String sBeginDate) {
            this.sBeginDate = sBeginDate;
            propertiesMap.put("sBeginDate", sBeginDate);
        }

        public void setsPartionDay(String sPartionDay) {
            this.sPartionDay = sPartionDay;
            propertiesMap.put("sPartionDay", sPartionDay);
        }

        public void setDateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
            propertiesMap.put("dateFormat", dateFormat);
        }

        public void setsEndDate(String sEndDate) {
            this.sEndDate = sEndDate;
            propertiesMap.put("sEndDate", sEndDate);
        }

        public void setDefaultNode(int defaultNode) {
            this.defaultNode = defaultNode;
            propertiesMap.put("defaultNode", String.valueOf(defaultNode));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NewPartitionByDate other = (NewPartitionByDate) o;
            return StringUtil.equals(other.sBeginDate, sBeginDate) &&
                StringUtil.equals(other.sPartionDay, sPartionDay) &&
                StringUtil.equals(other.dateFormat, dateFormat) &&
                StringUtil.equals(other.sEndDate, sEndDate) &&
                other.defaultNode == defaultNode;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private void initHashCode() {
            long tmpCode = beginDate;
            tmpCode *= partitionTime;
            if (defaultNode != 0) {
                tmpCode *= defaultNode;
            }
            if (!StringUtil.isEmpty(sEndDate)) {
                tmpCode *= endDate;
            }
            hashCode = (int) tmpCode;
        }

        @Override
        public String getDateFormat() {
            return dateFormat;
        }
    }
}
