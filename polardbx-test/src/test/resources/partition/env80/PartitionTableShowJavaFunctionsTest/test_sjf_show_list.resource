drop java function if exists sjf_func_alpha$
CREATE JAVA FUNCTION sjf_func_alpha
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Sjf_func_alpha extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a * 2;
  }
};
END_CODE$

drop java function if exists sjf_func_beta$
CREATE JAVA FUNCTION sjf_func_beta
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Sjf_func_beta extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a + b;
  }
};
END_CODE$

drop java function if exists sjf_func_gamma$
CREATE JAVA FUNCTION sjf_func_gamma
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Sjf_func_gamma extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a + 1;
  }
};
END_CODE$
