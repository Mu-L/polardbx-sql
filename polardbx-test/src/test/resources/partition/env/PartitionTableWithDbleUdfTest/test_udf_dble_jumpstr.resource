-- delimiter |
drop java function if exists jumpstrc5hs00|
CREATE JAVA FUNCTION jumpstrc5hs00
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(32)
CODE

  public class Jumpstrc5hs00 extends UserDefinedJavaFunction {

         private DblePartitionAlgorithm routeFunc;
         public Jumpstrc5hs00() {
             int count = 5;
             String hashSlice = "0:0";
             init(count, hashSlice);
         }

        private void init(int count, String hashSlice) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByJumpConsistentHash);
            params.setPartitionCount(String.valueOf(count));
            params.setHashSlice(hashSlice);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

        @Override
        public Object compute(Object[] args) {
            return routeFunc.compute(args);
        }
};
END_CODE|


drop java function if exists jumpstrc4hs00|
CREATE JAVA FUNCTION jumpstrc4hs00
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(32)
CODE

  public class Jumpstrc4hs00 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Jumpstrc4hs00() {
             int count = 4;
             String hashSlice = "0:0";
             init(count, hashSlice);
         }

        private void init(int count, String hashSlice) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByJumpConsistentHash);
            params.setPartitionCount(String.valueOf(count));
            params.setHashSlice(hashSlice);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

        @Override
        public Object compute(Object[] args) {
            return routeFunc.compute(args);
        }
};
END_CODE|
-- delimiter ;
