delimiter $
drop java function if exists unitmurhashc3 $
CREATE JAVA FUNCTION unitmurhashc3
  no state
  RETURN_TYPE bigint
  INPUT_TYPES VARCHAR(64)
CODE

  import java.nio.ByteBuffer;
  import java.nio.ByteOrder;
  import java.util.Random;
  public class Unitmurhashc3 extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public Unitmurhashc3() {
        String partitionCount = "2";
        String shardingValueMaxLength = "20";
        String isDigitCheck = "1";
        String leftPaddingChar = "A";
        init(partitionCount, isDigitCheck, leftPaddingChar, shardingValueMaxLength);
    }

    protected void init(String partitionCount,
                        String isDigitCheck,
                        String leftPaddingChar,
                        String shardingValueMaxLength) {
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_GH_PartitionByUnitMurmurHash);
        params.setPartitionCount(partitionCount);
        params.setIsDigitCheck(isDigitCheck);
        params.setLeftPaddingChar(leftPaddingChar);
        params.setShardingValueMaxLength(shardingValueMaxLength);
        this.routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return routeFunc.compute(args);
    }

  };
END_CODE $
delimiter ;

