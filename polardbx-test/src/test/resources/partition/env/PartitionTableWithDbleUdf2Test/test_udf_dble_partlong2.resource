delimiter $
drop java function if exists partlongi2l360 $
CREATE JAVA FUNCTION partlongi2l360
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Partlongi2l360 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partlongi2l360() {
             String count = "2";
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
END_CODE $
delimiter ;
