delimiter $
drop java function if exists unitmurhash8cd $
CREATE JAVA FUNCTION unitmurhash8cd
  no state
  RETURN_TYPE bigint
  INPUT_TYPES int
CODE

  import java.nio.ByteBuffer;
  import java.nio.ByteOrder;
  import java.util.Random;
  public class Unitmurhash8cd extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public Unitmurhash8cd() {
        String partitionCount = "8";
        String shardingValueMaxLength = "15";
        String isDigitCheck = "1";
        String leftPaddingChar = "NO";
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

