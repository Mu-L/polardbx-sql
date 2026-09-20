-- delimiter |
drop java function if exists pattrngd2p256i|
CREATE JAVA FUNCTION pattrngd2p256i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Pattrngd2p256i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngd2p256i() {
             int defaultNode = 2;
             int partitionValue = 256;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("1-32", 0));
             partMappings.add(new Pair<>("33-64", 1));
             partMappings.add(new Pair<>("65-96", 2));
             partMappings.add(new Pair<>("97-128", 3));
             partMappings.add(new Pair<>("129-160", 4));
             partMappings.add(new Pair<>("161-192", 5));
             partMappings.add(new Pair<>("193-224", 6));
             partMappings.add(new Pair<>("225-256", 7));
             partMappings.add(new Pair<>("0-0", 7));

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
