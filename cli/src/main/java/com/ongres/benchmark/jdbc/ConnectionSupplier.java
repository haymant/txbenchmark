/*-
 *  § 
 * benchmark: command-line
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

package com.ongres.benchmark.jdbc;

import java.io.Closeable;
import java.sql.Connection;
import java.util.function.Supplier;

public interface ConnectionSupplier extends Supplier<Connection>, Closeable {
  public static final ConnectionSupplier EMPTY = new EmptyConnectionSupplier();
}
