delimiter $
drop java function if exists pattrngid2p96 $
CREATE JAVA FUNCTION pattrngid2p96
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Pattrngid2p96 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngid2p96() {
             int defaultNode = 2;
             int partitionValue = 96;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("5-34", 0));
             partMappings.add(new Pair<>("30-60", 1));
             partMappings.add(new Pair<>("64-96", 2));

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


delimiter $
drop java function if exists pattrngip96 $
CREATE JAVA FUNCTION pattrngip96
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Pattrngip96 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngip96() {
             int defaultNode = -1;
             int partitionValue = 96;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("5-34", 0));
             partMappings.add(new Pair<>("30-60", 1));
             partMappings.add(new Pair<>("64-96", 2));

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


delimiter $
drop java function if exists pattrngstrd2p96 $
CREATE JAVA FUNCTION pattrngstrd2p96
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar
CODE

  public class Pattrngstrd2p96 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngstrd2p96() {
             int defaultNode = 2;
             int partitionValue = 96;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("5-34", 0));
             partMappings.add(new Pair<>("30-60", 1));
             partMappings.add(new Pair<>("64-96", 2));

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



delimiter $
drop java function if exists pattrngstrp96 $
CREATE JAVA FUNCTION pattrngstrp96
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar
CODE

  public class Pattrngstrp96 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Pattrngstrp96() {
             int defaultNode = -1;
             int partitionValue = 96;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("5-34", 0));
             partMappings.add(new Pair<>("30-60", 1));
             partMappings.add(new Pair<>("64-96", 2));

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


