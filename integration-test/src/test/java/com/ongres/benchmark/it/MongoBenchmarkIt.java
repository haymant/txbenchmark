/*-
 *  § 
 * benchmark: integration-test
 *    
 * Copyright (C) 2019 OnGres, Inc.
 *    
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * § §
 */

package com.ongres.benchmark.it;

import com.ongres.benchmark.App;
import com.ongres.junit.docker.Container;
import com.ongres.junit.docker.ContainerParam;
import com.ongres.junit.docker.DockerContainer;
import com.ongres.junit.docker.DockerExtension;
import com.ongres.junit.docker.Port;
import com.ongres.junit.docker.WaitFor;
import com.ongres.junit.docker.WhenReuse;

import org.junit.jupiter.api.Test;

@DockerExtension({
    @DockerContainer(
      alias = "mongo",
      image = "mongo:6",
      ports = { @Port(internal = 27017, external = 27017) },
      arguments = { "bash", "-c", 
          "(docker-entrypoint.sh mongod --bind_ip_all --replSet rs0) &"
              + " echo 'Initiating';"
              + " while ! mongosh --eval 'rs.initiate()'"
              + "    ; do sleep 1; done;"
              + " echo 'Adding Node';"
              + " while ! mongosh --eval 'rs.add(\"mongo:27017\")'"
              + "    ; do sleep 1; done;"
              + " for i in 1 2 3; do"
              + "   while ! mongosh --eval 'db.getCollectionNames()'"
              + "     ; do sleep 1; done;"
              + " sleep 1; done;"
              + " echo 'MongoDB is ready!';"
              + " seq -s ' ' 10000000 10000910;"
              + " wait" },
      waitFor = @WaitFor("MongoDB is ready!"),
      whenReuse = WhenReuse.ALWAYS)
})
public class MongoBenchmarkIt {

  @Test
  public void benchmarkTest(@ContainerParam("mongo") Container mongo) throws Exception {
    App.test(
        "--benchmark-target", "mongo", 
        "--parallelism", "20", 
        "--day-range", "1", 
        "--booking-sleep", "0", 
        "--target-database-port", "" + mongo.getPort(27017),
        "--target-database-user", "",
        "--target-database-name", "test",
        "--metrics", "PT10S",
        "--metrics-reporter", "log",
        "--duration", "PT60S");
  }
}
