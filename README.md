# Hadoop网络日志分析

## 描述

Web日志包含着网站最重要的信息，通过日志分析，我们可以知道网站的访问量，哪 个网页访问人数最多，哪个网页最有价值等。

### 1 日志文件结构

```
222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"
```

|     字段名      |                             解释                             |
| :-------------: | :----------------------------------------------------------: |
|   remote_addr   |              记录客户端的ip地址, 222.68.172.190              |
|   remote_user   |                    记录客户端用户名称, ––                    |
|   time_local    |       记录访问时间与时区, [18/Sep/2013:06:49:57 +0000]       |
|     request     |    记录请求的url与http协议, “GET /images/my.jpg HTTP/1.1”    |
|     status      |                    记录请求状态,成功是200                    |
| body_bytes_sent |           记录发送给客户端文件主体内容大小, 19939            |
|  http_referer   | 用来记录从那个页面链接访问过来的, “http://www.angularjs.cn/A00n” |
| http_user_agent | 记录客户浏览器的相关信息, “Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36” |

## Task1 对日志文件结构进行解析 

解析结果可参考如下格式:

```
remote_addr:222.68.172.190
remote_user:- time_local:18/Sep/2013:06:49:57 request:/images/my.jpg
status:200
body_bytes_sent:19939 http_referer:"http://www.angularjs.cn/A00n" http_user_agent:"Mozilla/5.0 (Windows 2013.09.18:06:49:57
2013091806 www.angularjs.cn
```

## Task2 网站资源的访问次数的统计 

统计每个资源路径的访问次数，以每个访问网页的资源路径为键，经过mapreduce任务， 最终得到每一个资源路径的访问次数。 

输出格式:`网站资源路径[\TAB]该资源路径被访问次数`

## Task3 访问网站的独立**IP**统计 

统计每个资源路径的ip访问，以每个访问网页的资源路径为键，经过mapreduce任务，最终得到每一个资源路径的访问ip的个数。 

输出格式:`网站资源路径[\TAB]访问该资源路径的ip的个数`

## Task4:每小时访问网站的次数统计 

用户每小时访问量统计。以每小时时间为键，每小时时间段内的访问次数进行累加即可得到结果。
输出格式:`每一小时的时间信息[\TAB]对应该一小时内网站的访问次数`
形如:

```
2013091806 111 
2013091807 1003 
2013091808 2040
```

可以看到日志文件前面一行对应2013年9月18日这一天每一小时的时间信息，后面的数字对应该一小时内网站的访问次数。

## Task5:访问网站的浏览器类型统计 

统计浏览器类型，输出了各种客户端对网站的访问情况。 

输出格式:`客户端浏览器类型[\TAB]该类型的客户数量`

## Task6 结果分析 

通过上述网站数据统计结果，进行排序汇总分析。