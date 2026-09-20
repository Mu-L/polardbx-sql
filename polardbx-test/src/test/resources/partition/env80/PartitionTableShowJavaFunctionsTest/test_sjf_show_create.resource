drop java function if exists sjf_cr_func1$
CREATE JAVA FUNCTION sjf_cr_func1
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Sjf_cr_func1 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a * 2;
  }
};
END_CODE$

drop java function if exists sjf_cr_func2$
CREATE JAVA FUNCTION sjf_cr_func2
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Sjf_cr_func2 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a + b;
  }
};
END_CODE$
