-- delimiter |
drop java function if exists partstrc2l512sm40|
CREATE JAVA FUNCTION partstrc2l512sm40
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(32)
CODE

  public class Partstrc2l512sm40 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partstrc2l512sm40() {
             String count = "2";
             String length = "512";
             String hashSlice = "-4:0";
             init(count, length, hashSlice);
         }

        private void init(String count, String partLen, String hashSlice) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByString);
            params.setPartitionCount(count);
            params.setPartitionLength(partLen);
            params.setHashSlice(hashSlice);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

        @Override
        public Object compute(Object[] args) {
            return  routeFunc.compute(args);
        }
};
END_CODE|
-- delimiter ;
