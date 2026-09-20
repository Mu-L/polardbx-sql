delimiter $
drop java function if exists partenumi $
CREATE JAVA FUNCTION partenumi
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Partenumi extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partenumi() {

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
END_CODE $
delimiter ;


delimiter $
drop java function if exists partenumi2 $
CREATE JAVA FUNCTION partenumi2
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Partenumi2 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partenumi2() {

             int type = 0;
             int defaultNode = -1;

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
END_CODE $
delimiter ;




delimiter $
drop java function if exists partenumstr $
CREATE JAVA FUNCTION partenumstr
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(32)
CODE

  public class Partenumstr extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partenumstr() {

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
END_CODE $
delimiter ;


delimiter $
drop java function if exists partenumstr2 $
CREATE JAVA FUNCTION partenumstr2
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(32)
CODE

  public class Partenumstr2 extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Partenumstr2() {

             int type = 1;
             int defaultNode = -1;

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
END_CODE $
delimiter ;