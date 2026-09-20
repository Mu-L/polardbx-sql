-- delimiter |
drop java function if exists pattrngd0p1024i|
CREATE JAVA FUNCTION pattrngd0p1024i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Pattrngd0p1024i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngd0p1024i() {
             int defaultNode = 0;
             int partitionValue = 1024;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("1-256", 0));
             partMappings.add(new Pair<>("257-512", 1));
             partMappings.add(new Pair<>("513-768", 2));
             partMappings.add(new Pair<>("769-1024", 3));
             partMappings.add(new Pair<>("0-0", 0));

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
END_CODE|
-- delimiter ;

