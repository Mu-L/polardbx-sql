-- delimiter |
drop java function if exists partdated0sp10d|
CREATE JAVA FUNCTION partdated0sp10d
  no state
  RETURN_TYPE bigint
  INPUT_TYPES datetime
CODE

  public class Partdated0sp10d extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partdated0sp10d() {
            int defaultNode = 0;
            String dateFormat = "yyyy-MM-dd";
            String sBeginDate = "2014-01-01";
            String sEndDate = "2014-01-30";
            String sPartionDay = "10";
            init(defaultNode, dateFormat, sBeginDate, sEndDate, sPartionDay);
         }

         private void init(int defaultNode,
                              String dateFormat,
                              String sBeginDate,
                              String sEndDate,
                              String sPartionDay) {
                BuildParams params = new BuildParams();
                params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByDate);
                params.setDefaultNode(defaultNode);
                params.setDateFormat(dateFormat);
                params.setsBeginDate(sBeginDate);
                params.setsEndDate(sEndDate);
                params.setsPartitionDay(sPartionDay);
                routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
            }

        @Override
        public Object compute(Object[] args) {
            return routeFunc.compute(args);
        }
};
END_CODE|
-- delimiter ;

