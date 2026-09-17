/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import org.jspecify.annotations.NonNull;
import scala.Tuple1;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes a roll up-style query against Apache logs.
 * Usage: JavaLogQuery [logFile]
 */
public final class JavaLogQuery {

  private static final List<String> exampleApacheLogs = Arrays.asList(
    "10.10.10.10 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg " +
      "HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
      "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
      ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
      "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" " +
      "62.24.11.25 images.com 1358492167 - Whatup",
    "10.10.10.10 - \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg " +
      "HTTP/1.1\" 304 306 \"http:/referall.com\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; " +
      "GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR " +
      "3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR  " +
      "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" " +
      "0 73.23.2.15 images.com 1358492557 - Whatup");

  private static final Pattern apacheLogRegex = Pattern.compile(
          "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{3,4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");
    private static final DateTimeFormatter ACCESS_LOG_TIME_FORMATTER =
          DateTimeFormatter.ofPattern("d/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

  private record AccessStatistics(String ip,
                                  OffsetDateTime startTime,
                                  OffsetDateTime endTime,
                                  long count,
                                  String query,
                                  String statusCode,
                                  long bytes,
                                  String userAgent,
                                  String users)
          implements Serializable, Comparable<AccessStatistics> {

    private AccessStatistics merge(AccessStatistics other) {
      String status = statusCode.contains(other.statusCode) ? statusCode : statusCode + "," + other.statusCode;
      String newUsers = users.contains(other.users) ? users : users + ", " + other.users;
      String newUserAgent = userAgent.contains(other.userAgent) ? userAgent : userAgent + ", " + other.userAgent;
      var start = startTime.isBefore(other.startTime) ? startTime : other.startTime;
      var end = endTime.isAfter(other.endTime) ? endTime : other.endTime;
      return new AccessStatistics(ip, start, end, count + other.count, query, status,
              bytes + other.bytes, newUserAgent, newUsers);
    }

    @Override
    public @NonNull String toString() {
      var user = (users.length() > 75) ? (users.substring(0, 75) + " ...") : users;
      var agents = (userAgent.length() > 50) ? (userAgent.substring(0, 50) + " ...") : userAgent;
      return String.format("ip=%s\tn=%s\tstart=%s\tend=%s\tstatus=%s\tkilobytes=%s\tusers=%s\tquery=%s\tagent=%s",
              ip, count, startTime, endTime,  statusCode, bytes/1000, user, query, agents);
    }

    @Override
    public int compareTo(AccessStatistics other) {
      return Long.compare(this.count, other.count);
    }
  }

  private static Tuple1<String> extractIpKey(String line) {
    Matcher m = apacheLogRegex.matcher(line);
    if (m.find()) {
      return new Tuple1<>(m.group(1));
    }
    return new Tuple1<>(null);
  }

  private static AccessStatistics extractCountUserStats(String line) {
    Matcher matcher = apacheLogRegex.matcher(line);
    if (matcher.find()) {
      String ip = matcher.group(1);
      String user = matcher.group(3);
      OffsetDateTime dateTime = getOffsetDateTime(matcher);
      String query = matcher.group(5);
      String status = matcher.group(6);
      long bytes = Long.parseLong(matcher.group(7));
      String userAgent = matcher.group(9);
      return new AccessStatistics(ip, dateTime, dateTime, 1, query ,
              status, bytes, userAgent, user);
    } else {
      return new AccessStatistics("", OffsetDateTime.MAX, OffsetDateTime.MIN, 1, "",
              "", 0, "", "");
    }
  }

  private static @NonNull OffsetDateTime getOffsetDateTime(Matcher m) {
    String timeStamp = m.group(4);
    // Normaliseer timezone: +000 -> +0000, -000 -> -0000
    timeStamp = timeStamp.replaceAll("([+\\-])(\\d{3})$", "$1$20");
    return OffsetDateTime.parse(timeStamp, ACCESS_LOG_TIME_FORMATTER);
  }

  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .appName("JavaLogQuery")
            .getOrCreate();

    JavaRDD<String> dataSet;
    try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
      long limit = (args.length >= 2) ? Long.parseLong(args[1]) : 1;
      Optional<String> statusCode = (args.length >= 3) ? Optional.of(args[2]) : Optional.empty();
      dataSet = (args.length >= 1) ? jsc.textFile(args[0]) : jsc.parallelize(exampleApacheLogs);
      //dataSet.repartition(80); werkt niet.

      generateResults(dataSet, limit, statusCode);
      System.in.read(); // to keep webUI up
      spark.stop();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }

  private static void generateResults(JavaRDD<String> dataSet, long limit, Optional<String> statusCodeFilter) {

    JavaPairRDD<Tuple1<String>, AccessStatistics> extracted =
            dataSet.mapToPair(s -> new Tuple2<>(extractIpKey(s), extractCountUserStats(s)));

    JavaPairRDD<Tuple1<String>, AccessStatistics> counts = extracted.reduceByKey(AccessStatistics::merge);

    List<AccessStatistics> allhits = new ArrayList<>(counts.values().collect());
    Collections.sort(allhits);
    var output = allhits
            .stream()
            .filter(statusCodeFilter.isPresent() ? stat -> stat.statusCode.contains(statusCodeFilter.get()) : s -> true)
            .filter(stat -> stat.count > limit)
            .toList();

    System.out.println("======================================================================================================");
    System.out.println("Results:");
    System.out.println("======================================================================================================");

    output.forEach(System.out::println);

    //System.out.println("======================================================================================================");
    //System.out.println("Number of requests: " + dataSet.count()); takes time.
    System.out.println("======================================================================================================");
    System.out.println("Number of distinct ip's: " + allhits.size());
    System.out.println("======================================================================================================");
    System.out.println("ip's with more than " + limit + " hits: " + output.size());
    System.out.println("======================================================================================================");
  }
}
