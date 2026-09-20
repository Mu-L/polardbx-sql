delimiter $
drop java function if exists partbyrangeudf $
CREATE JAVA FUNCTION partbyrangeudf
  no state
  RETURN_TYPE bigint
  INPUT_TYPES varchar(255)
CODE

  import java.io.BufferedReader;
  import java.io.ByteArrayInputStream;
  import java.io.InputStream;
  import java.io.InputStreamReader;
  import java.nio.charset.StandardCharsets;
  import java.util.HashMap;
  import java.util.HashSet;
  import java.util.Map;
  import java.util.Set;
  import java.util.regex.Matcher;
  import java.util.regex.Pattern;
  public class Partbyrangeudf extends UserDefinedParameterizedJavaFunction {
      private DblePartitionAlgorithm routeFunc;
      private Map<String, Object> algorithmParamsMappings = null;
      public Partbyrangeudf() {
      }

      @Override
      protected void initFunction(String initParams) {
          algorithmParamsMappings = (Map<String, Object>) DbleAlgorithmInitParams.buildMappingsFromJsonParams(initParams);
          String defaultNode = (String) algorithmParamsMappings.get("defaultNode");
          String mapFileString = (String) algorithmParamsMappings.get("mapFile");
          init(defaultNode, mapFileString);

      }

     @Override
     public Object getInitParamsInfo() {
        return algorithmParamsMappings;
     }

      private void init(String defaultNode, String mapFileString) {
        PartitionByRange newPartRange = new PartitionByRange();
        newPartRange.setDefaultNode(Integer.valueOf(defaultNode));
        newPartRange.setMapFileString(mapFileString);
        newPartRange.init();
        this.routeFunc = newPartRange;
      }

      public class PartitionByRange extends DblePartitionAlgorithm implements RuleAlgorithm {
          private static final long serialVersionUID = 8324680897275253917L;

          static final String RANGE_DEF_HEAD = "[range_definition]";

          static final String SEG_DEFAULT_HEAD = "[segments_default]";

          static final String RANGE_FORMAT = "(?<lb>\\d+)-(?<ub>\\d+)=(?<dn>-??\\d+)";

          static final char OUT_OF_BLK = '\000';

          static final char RANGE_DEF = '\001';

          static final char SEG_DEFAULT = '\002';

          static final int UNDEFINED_DATANODE = -2147483648;

          private String mapFile;

          private String mapFileString;

          private int defaultNode = -1;

          private long[] bounds;

          private int[] dataNodes;

          public PartitionByRange() {

          }

          private int binarySearch(long[] a, long key) {
              int low = 0;
              int high = a.length - 1;
              while (low <= high) {
                  int mid = low + high >>> 1;
                  long midVal = a[mid];
                  if (midVal < key) {
                      low = mid + 1;
                      continue;
                  }
                  if (midVal > key) {
                      high = mid - 1;
                      continue;
                  }
                  return mid + 1;
              }
              return low;
          }

          public Integer calculate(String columnValue) {
              try {
                  if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                      if (this.defaultNode >= 0)
                          return Integer.valueOf(this.defaultNode);
                      return null;
                  }
                  long value = Long.parseLong(columnValue);
                  int appIdx = binarySearch(this.bounds, value);
                  if (appIdx >= this.dataNodes.length) {
                      if (this.defaultNode >= 0)
                          return Integer.valueOf(this.defaultNode);
                      return null;
                  }
                  return Integer.valueOf(this.dataNodes[appIdx]);
              } catch (NumberFormatException e) {
                  throw new IllegalArgumentException(
                      "columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
              }
          }

          public Integer[] calculateRange(String beginValue, String endValue) {
            return null;
          }

          public int getPartitionNum() {
              Set<Integer> longRangeSet = new HashSet<>();
              for (int i = 0; i < this.dataNodes.length; i++)
                  longRangeSet.add(Integer.valueOf(this.dataNodes[i]));
              return longRangeSet.size();
          }

          public Map<String, String> getAllProperties() {
              return null;
          }

          public void init() {
              BufferedReader in = null;
              char blockType = Character.MIN_VALUE;
              Pattern rangePattern = Pattern.compile("(?<lb>\\d+)-(?<ub>\\d+)=(?<dn>-??\\d+)");
              Matcher rangeMatcher = null;
              Range ranges = null;
              Range segments = null;
              try {
                  this.mapFile = "mapFileInMemory";
                  this.defaultNode = Integer.valueOf(defaultNode);
                  byte[] dataBytes = mapFileString.getBytes(StandardCharsets.UTF_8);
                  InputStream inputStream = new ByteArrayInputStream(dataBytes);
                  InputStream fin = inputStream;
                  if (fin == null)
                      throw new RuntimeException("Cannot find mapFile file " + this.mapFile);
                  in = new BufferedReader(new InputStreamReader(fin));
                  for (String line = null; (line = in.readLine()) != null; ) {
                      line = line.trim();
                      if (line.startsWith("#") || line.startsWith("//") || "".equals(line))
                          continue;
                      if ("[range_definition]".equals(line)) {
                          blockType = '\001';
                          continue;
                      }
                      if ("[segments_default]".equals(line)) {
                          blockType = '\002';
                          continue;
                      }
                      rangeMatcher = rangePattern.matcher(line);
                      if (!rangeMatcher.matches())
                          throw new RuntimeException(String.format("Warn: malformed line in %s: %s\n", new Object[] { this.mapFile, line }));
                      long lowerBound = Long.parseLong(rangeMatcher.group("lb"));
                      long upperBound = Long.parseLong(rangeMatcher.group("ub"));
                      int dataNode = Integer.parseInt(rangeMatcher.group("dn"));
                      dataNode = (dataNode < 0) ? this.defaultNode : dataNode;
                      if (lowerBound > upperBound)
                          throw new RuntimeException(
                              String.format("Left value cannot be bigger than right value in %s: %s\n", new Object[] { this.mapFile, line }));
                      Range node = new Range(lowerBound, upperBound, dataNode);
                      if ('\001' == blockType) {
                          ranges = Range.joinListWithOrder(ranges, node);
                          continue;
                      }
                      segments = Range.joinListWithOrder(segments, node);
                  }
              } catch (Exception e) {
                  if (e instanceof RuntimeException)
                      throw (RuntimeException)e;
                  throw new RuntimeException(e);
              } finally {
                  if (in != null)
                      try {
                          in.close();
                      } catch (Exception exception) {}
              }
              if (ranges == null)
                  throw new RuntimeException(
                      String.format("There MUST be one or more range definition after %s in your mapFile: %s\n", new Object[] { "[range_definition]", this.mapFile }));
              Range.makeListContinuous(segments);
              Range.makeListContinuous(ranges);
              ranges = Range.applySegmentsDefault(segments, ranges);
              ranges = Range.optimize(ranges, this.defaultNode);
              int rangeCount = ranges.count();
              int i = 1;
              this.bounds = new long[rangeCount + 1];
              this.dataNodes = new int[rangeCount + 1];
              this.bounds[0] = ranges.lowerBound;
              this.dataNodes[0] = this.defaultNode;
              while (ranges != null) {
                  this.bounds[i] = ranges.upperBound + 1L;
                  this.dataNodes[i++] = ranges.datanode;
                  ranges = ranges.next;
              }
          }

          public void selfCheck() {}

          public void setDefaultNode(int defaultNode) {
              this.defaultNode = defaultNode;
          }

          public void setMapFile(String mapFile) {
              this.mapFile = mapFile;
          }

          public void setMapFileString(String mapFileString) {
            this.mapFileString = mapFileString;
          }

      }

      private static class Range {
          public long lowerBound;

          public long upperBound;

          public int datanode;

          private Range previous = null;

          private Range next = null;

          public static void append(Range r1, Range r2) {
              r2.previous = r1;
              r2.next = r1.next;
              if (r1.next != null)
                  r1.next.previous = r2;
              r1.next = r2;
          }

          public static Range applySegmentsDefault(Range segments, Range ranges) {
              Range seg = segments;
              Range rng = ranges;
              if (segments == null || ranges == null)
                  return null;
              while (rng != null) {
                  if (!rng.hasPrevious() &&
                      segments.lowerBound < rng.lowerBound && segments.upperBound >= rng.lowerBound - 1L) {
                      insert(rng, new Range(segments.lowerBound, rng.lowerBound - 1L, segments.datanode));
                      continue;
                  }
                  if (Integer.MIN_VALUE != rng.datanode) {
                      rng = rng.next;
                      continue;
                  }
                  while (seg != null) {
                      if (rng.upperBound < seg.lowerBound)
                          break;
                      if (rng.lowerBound > seg.upperBound) {
                          seg = seg.next;
                          continue;
                      }
                      if (rng.lowerBound == seg.upperBound) {
                          append(rng, new Range(rng.lowerBound + 1L, rng.upperBound, rng.datanode));
                          rng.upperBound = rng.lowerBound;
                          rng.datanode = seg.datanode;
                          rng = rng.next;
                          seg = seg.next;
                          continue;
                      }
                      if (rng.upperBound == seg.lowerBound) {
                          insert(rng, new Range(rng.lowerBound, rng.upperBound - 1L, rng.datanode));
                          rng.lowerBound = seg.lowerBound;
                          rng.datanode = seg.datanode;
                          break;
                      }
                      if (rng.upperBound > seg.upperBound) {
                          if (rng.lowerBound < seg.lowerBound)
                              insert(rng, new Range(rng.lowerBound, seg.lowerBound - 1L, rng.datanode));
                          insert(rng,
                              new Range(Math.max(rng.lowerBound, seg.lowerBound), seg.upperBound, seg.datanode));
                          rng.lowerBound = seg.upperBound + 1L;
                          seg = seg.next;
                          continue;
                      }
                      if (rng.lowerBound < seg.lowerBound)
                          insert(rng, new Range(rng.lowerBound, seg.lowerBound - 1L, rng.datanode));
                      rng.lowerBound = Math.max(rng.lowerBound, seg.lowerBound);
                      rng.upperBound = rng.upperBound;
                      rng.datanode = seg.datanode;
                      break;
                  }
                  rng = rng.next;
              }
              rng = getLast(ranges);
              while (seg != null) {
                  if (rng.upperBound >= seg.upperBound) {
                      seg = seg.next;
                      continue;
                  }
                  append(rng, new Range(rng.upperBound + 1L, seg.upperBound, seg.datanode));
                  rng = rng.next;
                  seg = seg.next;
              }
              return getFirst(ranges);
          }

          public static Range getFirst(Range list) {
              if (list == null)
                  return null;
              while (list.hasPrevious())
                  list = list.previous;
              return list;
          }

          public static Range getLast(Range list) {
              if (list == null)
                  return null;
              while (list.hasNext())
                  list = list.next;
              return list;
          }

          public static void insert(Range r1, Range r2) {
              r2.previous = r1.previous;
              r2.next = r1;
              if (r1.previous != null)
                  r1.previous.next = r2;
              r1.previous = r2;
          }

          public static Range joinListWithOrder(Range list, Range r) {
              if (list == null)
                  return r;
              Range cur = null;
              do {
                  cur = (cur != null) ? cur.next : list;
                  if (cur.lowerBound > r.upperBound) {
                      insert(cur, r);
                      return list;
                  }
                  if ((cur.lowerBound <= r.lowerBound && r.lowerBound <= cur.upperBound) || (
                      cur.lowerBound <= r.upperBound && r.upperBound <= cur.upperBound))
                      throw new RuntimeException(
                          String.format("Range %s conflicts with range %s, please check your mapFile.", new Object[] { cur, r }));
              } while (cur.hasNext());
              append(cur, r);
              return list;
          }

          public static void makeListContinuous(Range list) {
              Range cur = null;
              do {
                  cur = (cur != null) ? cur.next : list;
                  if (!cur.hasPrevious())
                      continue;
                  if (cur.previous.upperBound == cur.lowerBound - 1L)
                      continue;
                  insert(cur, new Range(cur.previous.upperBound + 1L, cur.lowerBound - 1L, -2147483648));
              } while (cur.hasNext());
          }

          public static Range optimize(Range list, int globalDefault) {
              if (list == null)
                  return null;
              Range i = list;
              while (i != null) {
                  if (Integer.MIN_VALUE == i.datanode)
                      i.datanode = globalDefault;
                  if (i.previous == null) {
                      i = i.next;
                      continue;
                  }
                  if (i.previous.datanode == i.datanode && i.previous.upperBound + 1L == i.lowerBound) {
                      i.previous.upperBound = i.upperBound;
                      i.previous.next = i.next;
                      if (i.next != null)
                          i.next.previous = i.previous;
                  }
                  i = i.next;
              }
              return getFirst(list);
          }

          public Range(long lb, long ub, int dn) {
              this.lowerBound = lb;
              this.upperBound = ub;
              this.datanode = dn;
          }

          public int count() {
              int c = 1;
              Range i = this;
              while (i.hasNext()) {
                  i = i.next;
                  c++;
              }
              return c;
          }

          public boolean hasNext() {
              return (this.next != null);
          }

          public boolean hasPrevious() {
              return (this.previous != null);
          }

          public String toString() {
              return String.format("%d-%d=%d", new Object[] { Long.valueOf(this.lowerBound), Long.valueOf(this.upperBound), Integer.valueOf(this.datanode) });
          }
      }

      @Override
      public Object compute(Object[] args) {
          if (!isInited) {
            throw new RuntimeException(String.format("udf function %s has not init yet", getFunctionNames()[0]));
          }
          return  routeFunc.compute(args);
      }

};
END_CODE $
delimiter ;
