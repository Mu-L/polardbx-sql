delimiter $
drop java function if exists pattrngip10 $
CREATE JAVA FUNCTION pattrngip10
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Pattrngip10 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngip10() {
             int defaultNode = -1;
             int partitionValue = 10;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("4-6", 0));
             partMappings.add(new Pair<>("7-8", 1));
             partMappings.add(new Pair<>("9-10", 2));

             init(defaultNode, partitionValue, partMappings);
         }

        private void init(int defaultNode, int partitionValue, List<Pair<String, Integer>> partitionMappings) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByPattern);
            params.setDefaultNode(defaultNode);
            params.setPartitionValue(partitionValue);
            params.setPartitionMappings(partitionMappings);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

        @Override
        public Object compute(Object[] args) {
            return routeFunc.compute(args);
        }
};
END_CODE $
delimiter ;