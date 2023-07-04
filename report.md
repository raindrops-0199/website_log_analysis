## 任务三

### 输入与输出

- 输入：以任务1的输出文件作为输入。文件格式为每9行为一个单位，里面记录了一个log中包含的信息，格式如下：

  ```json
  remote_addr:	1.162.203.134
  remote_user:	-
  time_local:	18/Sep/2013:13:47:35
  request:	/images/my.jpg
  status:	200
  body_bytes_sent:	19939
  http_referer:	http://www.angularjs.cn/A0d9
  http_user_agent:	Mozilla/5.0 
  date:	2013091813
  ```

- 输出：每个网站资源访问的IP个数，格式：网站资源路径[\TAB]访问该资源路径的ip的个数

### 自定义输出格式

因为格式不是每行为一个单位，而是每9行一个单位，所以自定义InputFormat和RecordReader用来处理输入。

**InputFormat**

```java
public class ResInputFormat extends FileInputFormat {
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        ResRecordReader reader = new ResRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}
```

**RecordReader**

ResRecordReader继承LineRecordReader，重新实现nextKeyValue方法，使得每9行读取一次，将读取的结果用";"拼接。

```java
public class ResRecordReader extends LineRecordReader {
    private Text resValue = new Text();
    private LongWritable resKey = new LongWritable();

    public boolean nextKeyValue() throws IOException {
        StringBuilder valueBuilder = new StringBuilder();
        int count = 0;
        while (super.nextKeyValue()) {
            LongWritable key = super.getCurrentKey();
            Text value = super.getCurrentValue();
            resKey.set(key.get());
            valueBuilder.append(value.toString()).append(";");
            count++;
            if (count == 9) {
                resValue.set(valueBuilder.toString());
                return true;
            }
        }
        if (count > 0) {
            resValue.set(valueBuilder.toString());
            return true;
        }
        return false;
    }
}
```

**main函数设置**

在main函数中设置自定义的输入格式

```java
job.setInputFormatClass(ResInputFormat.class);
```

### Map阶段

map的输入输出：<*LongWritable*, *Text*, *Text*, *Text*>

自定义RecordReader的key value类型为LongWritable和Text，所以Map的输入类型为这两个。Map的输出key- value是路径类型-IP，所以都以Text格式存储。

**算法**

对输入的每组log信息，对其进行解析，获取资源路径和IP，然后输出。对于HEAD请求，因为该请求并未直接获取某资源的内容，所以对其忽略。

**代码**

```java
private static class Task3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text resource = new Text();
    private Text IpText = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 解析输入
        String[] splits = value.toString().split(";");
        String[] requestF = splits[3].split("\t");
        if (!(requestF.length > 1)) {
            return;
        }
        String request = requestF[1];
        // 忽略HEAD请求
        if (!request.equals("-") && !request.equals("/")) {
            // 对有资源和IP的输入，获取其资源路径与IP，然后输出
            String ip = splits[0].split("\t")[1];
            resource.set(request);
            IpText.set(ip);
            context.write(resource, IpText);
        }
    }
}
```

### Reduce阶段

reduce阶段的输入输出：<*Text*, *Text*, *Text*, *IntWritable*>

输入为Map阶段的输出，所以格式为Text与Text， Reduce的输出为资源路径对应IP个数，所以用Text-IntWritable

**算法**

用一个HashSet存储IP，保证同一资源路径下的IP都存入后，获取其size，就是该资源路径的访问IP个数。然后进行输出即可。

**代码**

```java
private static class Task3Reducer extends Reducer<Text, Text, Text, IntWritable> {
    private IntWritable outVal = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> IpSet = new HashSet<>();
        // 将IP加入HashSet中
        for (Text text : values) {
            IpSet.add(text.toString());
        }
        // 获取size
        outVal.set(IpSet.size());
        //输出
        context.write(key, outVal);
    }
}
```

### 输出结果截图

![image-20230704214134552](https://hachaosg2p-1308589153.cos.ap-nanjing.myqcloud.com/202307042141660.png)

### MapReduce执行信息截图

//待完善

## 任务四

### 输入与输出

- 输入是任务1的输出，格式同上。该任务无需使用自定义输入，因为只需要`date`行的信息，无需按组获取信息。

- 输出：每小时网站访问次数。格式：每一小时的时间信息[\TAB]对应该一小时内网站的访问次数。

  时间已在任务1中格式化输出为`2013091806`的，以小时为最小单位的格式类型，所以在该任务中直接使用即可。

### Map阶段

Map的输入输出格式：<*LongWritable*, *Text*, *Text*, *IntWritable*>

使用默认的LineRecordReader作为输出格式，所以输入为LongWritable-Text。输出为时间和次数1，使用Text存储时间，用IntWritabale存储次数。

**算法**

在Map阶段，对每个日期，直接输出<日期, 1>的键值对即可，表示该日期出现了1次访问。

**代码**

```java
private static class Task4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text date = new Text();
    // 固定为1
    private final IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strVal = value.toString();
        // 如果输入的行以date起始，就是我们要的信息
        if (strVal.startsWith("date")) {
            // 获取date数据
            date.set(strVal.split("\t")[1]);
            // 输出
            context.write(date, one);
        }
    }
}
```

### Combiner

为减少Map和Reduce节点之间的信息开销，可使用Combiner将日期相同的访问此处相加，然后再发给Reduce节点。

Combiner的输入输出：<*Text*, *IntWritable*, *Text*, *IntWritable*>

输入为Map的输出格式，输出为日期和访问次数，日期用Text，访问次数用IntWritable

**算法**

Combiner将相同日期的访问次数相加，然后输出<日期, 访问次数>键值对即可。

**代码**

```java
private static class Task4Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable value = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // 将访问次数相加
        for (IntWritable item : values) {
            count += item.get();
        }
        // 输出
        value.set(count);
        context.write(key, value);
    }
}
```

**main函数设置Combiner**

```java
job.setCombinerClass(Task4Reducer.class);
```

### Reduce阶段

Reduce阶段也是将相同日期的访问次数相加。所以与Combiner阶段的输入输出类型，以及代码完全相同。只需在main函数中设置即可：

```java
job.setReducerClass(Task4Reducer.class);
```

Reduce阶段输出<日期, 访问次数>键值对至输出文件中。

### 输出结果截图

![image-20230704214156020](https://hachaosg2p-1308589153.cos.ap-nanjing.myqcloud.com/202307042141083.png)

### MapReduce执行信息截图

//待完善