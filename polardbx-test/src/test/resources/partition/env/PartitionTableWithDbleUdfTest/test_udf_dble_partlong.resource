-- delimiter |
drop java function if exists partlongc8l360|
CREATE JAVA FUNCTION Partlongc8l360
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(255)
CODE

  public class Partlongc8l360 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partlongc8l360() {
             String count = "8";
             String length = "360";
             init(count,length);
         }

         private void init(String count, String partLen) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByLong);
            params.setPartitionCount(count);
            params.setPartitionLength(partLen);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
         }

         @Override
         public Object compute(Object[] args) {
            return  routeFunc.compute(args);
         }
};
END_CODE|

drop java function if exists partlongc1c2l1l2i|
CREATE JAVA FUNCTION partlongc1c2l1l2i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Partlongc1c2l1l2i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partlongc1c2l1l2i() {
             String count = "1,2";
             String length = "1,2";
             init(count, length);
         }

         private void init(String count, String partLen) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByLong);
            params.setPartitionCount(count);
            params.setPartitionLength(partLen);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
         }

         @Override
         public Object compute(Object[] args) {
            return  routeFunc.compute(args);
         }
};
END_CODE|
-- delimiter ;
