## Java Examples for Stream Processing with Apache Flink

This repository hosts Java code examples for ["Stream Processing with Apache Flink"](http://shop.oreilly.com/product/0636920057321.do) by [Fabian Hueske](https://twitter.com/fhueske) and [Vasia Kalavri](https://twitter.com/vkalavri).

**Note:** The Java examples are not comlete yet. <br>
The [Scala examples](https://github.com/streaming-with-flink/examples-scala) are complete and we are working on translating them to Java.

<a href="http://shop.oreilly.com/product/0636920057321.do">
  <img width="240" src="https://covers.oreillystatic.com/images/0636920057321/cat.gif">
</a>


## Command

```bash
cd /media/henry.wu/sandbox/git.repo/flink-examples/src/main
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf \
 --java_out=java resources/mazu-kubernetes-podwatcher-raw.proto
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf \
 --java_out=java resources/mazu-kubernetes-podwatcher-enriched.proto
```


Data Stream:
```
pod_name, job_name, multiplier, start_time, end_time
0, 1, 0.1, 0, -1
1, 1, 0.1, 0, -1
2, 2, 0.1, 0, -1
3, 2, 0.1, 0, -1
0, 1, 0.1, 0, 1
```

`dataStream.filter(end_time<0)` will generate this data:

```$xslt
pod_name, job_name, multiplier, start_time, end_time
0, 1, 0.1, 0, -1
1, 1, 0.1, 0, -1
2, 2, 0.1, 0, -1
3, 2, 0.1, 0, -1
```

When the fifth row `0, 1, 0.1, 0, 1` comes, we want remove(retract) the first row, and then we get two streams:

(running pods stream)
```$xslt
pod_name, job_name, multiplier, start_time, end_time
1, 1, 0.1, 0, -1
2, 2, 0.1, 0, -1
3, 2, 0.1, 0, -1
```

and:

(finished pods stream)
```$xslt
pod_name, job_name, multiplier, start_time, end_time
0, 1, 0.1, 0, 1
```