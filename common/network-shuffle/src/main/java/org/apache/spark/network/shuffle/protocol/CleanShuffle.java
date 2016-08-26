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
package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import com.google.common.base.Objects;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

public class CleanShuffle extends BlockTransferMessage {
  public final String appId;
  public final String shuffleId;
  public final String userName;
  public final String[] toRemoveExecutorId;

  public CleanShuffle(String appId, String shuffleId, String user, String[] toRemoveExecutorId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.userName = user;
    this.toRemoveExecutorId = toRemoveExecutorId;
  }

  @Override
  protected Type type() {
    return Type.CLEAN_SHUFFLE;
  }

  public String getAppId() {
    return appId;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(shuffleId)
      + Encoders.Strings.encodedLength(userName)
      + Encoders.StringArrays.encodedLength(toRemoveExecutorId);  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, shuffleId);
    Encoders.Strings.encode(buf, userName);
    Encoders.StringArrays.encode(buf, toRemoveExecutorId);
  }


  @Override
  public int hashCode() {
    return Objects.hashCode(appId, shuffleId, userName) * 41 + Arrays.hashCode(toRemoveExecutorId);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("shuffleId", shuffleId)
      .add("userName", userName)
      .add("toRemoveExecutorId", Arrays.toString(toRemoveExecutorId))
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof CleanShuffle) {
      CleanShuffle o = (CleanShuffle) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(shuffleId, o.shuffleId)
        && Objects.equal(userName, o.userName)
        && Arrays.equals(toRemoveExecutorId, o.toRemoveExecutorId);
    }
    return false;
  }

  public static CleanShuffle decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String shuffleId = Encoders.Strings.decode(buf);
    String userName = Encoders.Strings.decode(buf);
    String[] toRemoveExecutorId = Encoders.StringArrays.decode(buf);

    return new CleanShuffle(appId, shuffleId, userName, toRemoveExecutorId);
  }

}
