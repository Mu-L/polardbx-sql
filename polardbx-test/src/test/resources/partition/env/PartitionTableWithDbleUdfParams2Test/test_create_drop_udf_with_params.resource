DELIMITER $
DROP JAVA FUNCTION IF EXISTS mymod $
CREATE JAVA FUNCTION mymod
  RETURN_TYPE BIGINT
  INPUT_TYPES VARCHAR(255)
CODE
  public class Mymod extends UserDefinedParameterizedJavaFunction {
        protected Long partitionCountVal = 0L;
        protected Map<String, Object> algorithmParamsMappings = new HashMap<String, Object>();
        public Mymod() {
        }

        @Override
        public void initFunction(String initParams) {
            this.algorithmParamsMappings = (Map<String, Object>) DbleAlgorithmInitParams.buildMappingsFromJsonParams(initParams);
            String partitionCount = (String) algorithmParamsMappings.get("partitionCount");
            initMyMod(partitionCount);
        }

        protected void initMyMod(String count) {
            this.partitionCountVal = Long.valueOf(count);
        }

        protected Long doMyMod(String inputVal) {
            Long hashVal = Long.valueOf(inputVal) % partitionCountVal;
            return 0L;
        }

        @Override
        public Object compute(Object[] args) {
            if (!isInited) {
               throw new RuntimeException(String.format("udf function does not init yet"));
            }
            String inputVal = (String) args[0];
            Long hashVal = (Long) doMyMod(inputVal);
            return hashVal;
        }

        @Override
        public Object getInitParamsInfo() {
              return algorithmParamsMappings;
        }
};
END_CODE $
DELIMITER ;


DELIMITER $
DROP JAVA FUNCTION IF EXISTS mymod $
CREATE JAVA FUNCTION mymod
  RETURN_TYPE BIGINT
  INPUT_TYPES VARCHAR(255)
CODE
  public class Mymod extends UserDefinedParameterizedJavaFunction {
        protected Long partitionCountVal = 0L;
        protected Map<String, Object> algorithmParamsMappings = new HashMap<String, Object>();
        public Mymod() {
        }

        @Override
        public void initFunction(String initParams) {
            this.algorithmParamsMappings = (Map<String, Object>) DbleAlgorithmInitParams.buildMappingsFromJsonParams(initParams);
            String partitionCount = (String) algorithmParamsMappings.get("partitionCount");
            initMyMod(partitionCount);
        }

        protected void initMyMod(String count) {
            this.partitionCountVal = Long.valueOf(count);
        }

        protected Long doMyMod(String inputVal) {
            Long hashVal = Long.valueOf(inputVal) % partitionCountVal;
            return hashVal;
        }

        @Override
        public Object compute(Object[] args) {
            if (!isInited) {
               throw new RuntimeException(String.format("udf function does not init yet"));
            }
            String inputVal = (String) args[0];
            Long hashVal = (Long) doMyMod(inputVal);
            return hashVal;
        }

        @Override
        public Object getInitParamsInfo() {
              return algorithmParamsMappings;
        }
};
END_CODE $
DELIMITER;
