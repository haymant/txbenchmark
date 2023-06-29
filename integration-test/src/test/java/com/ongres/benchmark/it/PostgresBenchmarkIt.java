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
import com.ongres.junit.docker.Environment;
import com.ongres.junit.docker.Port;
import com.ongres.junit.docker.WaitFor;
import com.ongres.junit.docker.WhenReuse;

import org.junit.jupiter.api.Test;

@DockerExtension({
    @DockerContainer(
        alias = "postgres",
        image = "postgres:15",
        ports = { @Port(internal = 5432) },
        environments = { @Environment(key = "POSTGRES_HOST_AUTH_METHOD", value = "trust")},
        arguments = { "bash", "-c", 
            "(docker-entrypoint.sh postgres) &"
                + " for i in 1 2 3; do"
                + "   while ! psql -h localhost -U postgres"
                + "     -c 'SELECT 1' > /dev/null 2>&1; do sleep 1; done;"
                + " sleep 1; done;"
                + " echo 'PostgreSQL is ready!';"
                + " seq -s ' ' 10000000 10000910;"
                + " wait" },
        waitFor = @WaitFor("PostgreSQL is ready!"),
        whenReuse = WhenReuse.ALWAYS)
})
public class PostgresBenchmarkIt {
  @Test
  public void benchmarkTest(@ContainerParam("postgres") Container postgres) throws Exception {
    App.test(
        "--benchmark-target", "postgres", 
        "--parallelism", "20", 
        "--day-range", "30", 
        "--booking-sleep", "0", 
        "--connection-wait-timeout", "PT0S", 
        "--target-database-port", "" + postgres.getPort(5432),
        "--metrics", "PT10S",
        "--metrics-reporter", "log",
        "--duration", "PT60S");
  }
}
