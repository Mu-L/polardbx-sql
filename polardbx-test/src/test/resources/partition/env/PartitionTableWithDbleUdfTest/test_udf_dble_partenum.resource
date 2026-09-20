-- delimiter |
drop java function if exists partenumt0d0i|
CREATE JAVA FUNCTION partenumt0d0i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Partenumt0d0i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partenumt0d0i() {

             int type = 0;
             int defaultNode = 0;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("10000", 0));
             partMappings.add(new Pair<>("10010", 1));

             init(type, defaultNode, partMappings);
         }

        protected void init(int type, int defaultNode, List<Pair<String, Integer>> mappings) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByFileMap);
            params.setType(type);
            params.setDefaultNode(defaultNode);
            params.setPartitionMappings(mappings);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

        @Override
        public Object compute(Object[] args) {
            return  routeFunc.compute(args);
        }
};
END_CODE|
-- delimiter ;

-- delimiter |
drop java function if exists partenumtm1d0c|
CREATE JAVA FUNCTION partenumtm1d0c
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(32)
CODE

  public class Partenumtm1d0c extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partenumtm1d0c() {

             int type = 1;
             int defaultNode = 0;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("A", 0));
             partMappings.add(new Pair<>("B", 1));

             init(type, defaultNode, partMappings);
         }

        protected void init(int type, int defaultNode, List<Pair<String, Integer>> mappings) {
            BuildParams params = new BuildParams();
            params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByFileMap);
            params.setType(type);
            params.setDefaultNode(defaultNode);
            params.setPartitionMappings(mappings);
            routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
        }

        @Override
        public Object compute(Object[] args) {
            return  routeFunc.compute(args);
        }
};
END_CODE|
-- delimiter ;
