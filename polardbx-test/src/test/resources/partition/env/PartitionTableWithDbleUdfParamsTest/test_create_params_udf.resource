delimiter $
drop java function if exists myparamsfunc $
CREATE JAVA FUNCTION myparamsfunc
  RETURN_TYPE bigint
  INPUT_TYPES varchar
  CODE
  public class Myparamsfunc extends UserDefinedParameterizedJavaFunction {

      protected String initParams = "";
      public Myparamsfunc() {

      }

      @Override
      public void initFunction(String params) {
            initParams = params;
      }

      @Override
      public Object getInitParamsInfo() {
            return initParams;
      }

      @Override
      public Object compute(Object[] args) {
        String a = (String) args[0];
        Long val = Long.valueOf(a + initParams);
        return val;
      }
  };
END_CODE $

delimiter ;
