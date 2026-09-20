delimiter $
drop java function if exists myudf1$
drop java function if exists myudf1$
CREATE JAVA FUNCTION IF NOT EXISTS myudf1
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Myudf1 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a;
  }
};
END_CODE$

CREATE JAVA FUNCTION IF NOT EXISTS myudf1
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Myudf1 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a;
  }
};
END_CODE$
drop java function if exists myudf1$
drop java function if exists myudf1$
CREATE JAVA FUNCTION IF NOT EXISTS myudf1
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
  CODE
  public class Myudf1 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    return a;
  }
};
END_CODE$

drop java function if exists myudf2$
CREATE JAVA FUNCTION IF NOT EXISTS myudf2
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint,bigint
  CODE
  public class Myudf2 extends UserDefinedJavaFunction {
  public Object compute(Object[] args) {
    Long a = (Long) args[0];
    Long b = (Long) args[1];
    return a+b;
  }
};
END_CODE$
delimiter ;