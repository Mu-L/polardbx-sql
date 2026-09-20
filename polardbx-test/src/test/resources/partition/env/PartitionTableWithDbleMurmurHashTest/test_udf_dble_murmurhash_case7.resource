delimiter $
drop java function if exists unitmurhashc7 $
CREATE JAVA FUNCTION unitmurhashc7
  no state
  RETURN_TYPE bigint
  INPUT_TYPES VARCHAR(64)
CODE

  import java.nio.ByteBuffer;
  import java.nio.ByteOrder;
  import java.util.Random;
  public class Unitmurhashc7 extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public Unitmurhashc7() {
        String partitionCount = "2";
        String shardingValueMaxLength = "20";
        String isDigitCheck = "1";
        String leftPaddingChar = null;
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

