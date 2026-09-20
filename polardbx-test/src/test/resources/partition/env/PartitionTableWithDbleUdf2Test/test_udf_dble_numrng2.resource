delimiter $
drop java function if exists numrngid0r3 $
CREATE JAVA FUNCTION numrngid0r3
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Numrngid0r3 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Numrngid0r3() {
             int defaultNode = 0;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("0-200M", 0));
             partMappings.add(new Pair<>("200M1-400M", 1));
             partMappings.add(new Pair<>("400M1-600M", 2));

             init(defaultNode, partMappings);
         }

        private void init(int defaultNode, List<Pair<String, Integer>> partitionMappings) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_AutoPartitionByLong);
            params.setDefaultNode(defaultNode);
            params.setPartitionMappings(partitionMappings);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

         @Override
         public Object compute(Object[] args) {
             Long val = (Long) args[0];
             Integer rs = routeFunc.calculate(String.valueOf(val));
             return rs;
         }
};
END_CODE $
delimiter ;
