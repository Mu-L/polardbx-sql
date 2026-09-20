delimiter $
drop java function if exists unitmurhash16 $
CREATE JAVA FUNCTION unitmurhash16
  no state
  RETURN_TYPE bigint
  INPUT_TYPES VARCHAR(32)
CODE

  import java.nio.ByteBuffer;
  import java.nio.ByteOrder;
  import java.util.Random;
  public class Unitmurhash16 extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public Unitmurhash16() {
        String partitionCount = "16";
        String isDigitCheck = null;
        String leftPaddingChar = null;
        String shardingValueMaxLength = null;
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

