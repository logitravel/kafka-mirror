# kafka-mirror

This Apache Storm topology replicates messages from a kafka cluster to another one.

## Build for deployment
```bash
gradle fatJar
```

## Usage
```bash
usage: com.logitravel.kafka.mirror.Topology
 -c,--consumer.config <consumer.config>   Consumer config file
 -e,--executors <executors>               Executors (default: 3)
 -n,--name <name>                         Topology name, required for deployment
 -p,--producer.config <producer.config>   Producer config file
 -t,--topic <topic>                       Topic(s). Comma separated topic1,topic2, ...
 -w,--workers <workers>                   Workers (default: 3)
```

### Example deployment command
```bash
$PATH_TO_STORM/bin/storm jar build/libs/kafka-mirror-<VERSION>-jar-with-dependencies.jar com.logitravel.kafka.mirror.Topology \
  --consumer.config consumer.properties \
  --producer.config producer.properties \
  --topic successEvents,errorEvents
  
```

### Links

[Apache Kafka](http://kafka.apache.org/090/documentation.html)

[Apache Storm](http://storm.apache.org/)

### License
Copyright 2016 Logitravel

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.