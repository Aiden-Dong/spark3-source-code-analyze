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

package org.apache.spark.resource

import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction, JValue}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Evolving

/**
 * 用于保存某种资源信息的类。资源可以是 GPU、FPGA 等。
 * 地址数组是特定于资源的，由用户解释地址含义。
 *
 * 一个例子是 GPU，其中地址是 GPU 的索引。
 *
 * @param name 资源名称
 * @param addresses 描述资源地址的字符串数组
 *
 * @since 3.0.0
 */
@Evolving
class ResourceInformation(
    val name: String,
    val addresses: Array[String]) extends Serializable {

  override def toString: String = s"[name: ${name}, addresses: ${addresses.mkString(",")}]"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceInformation =>
        that.getClass == this.getClass &&
        that.name == name && that.addresses.toSeq == addresses.toSeq
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(name, addresses.toSeq).hashCode()

  def toJson(): JValue = ResourceInformationJson(name, addresses).toJValue
}

private[spark] object ResourceInformation {

  private lazy val exampleJson: String = compact(render(
    ResourceInformationJson("gpu", Seq("0", "1")).toJValue))

  /**
   * Parses a JSON string into a [[ResourceInformation]] instance.
   */
  def parseJson(json: String): ResourceInformation = {
    implicit val formats = DefaultFormats
    try {
      parse(json).extract[ResourceInformationJson].toResourceInformation
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n" +
          s"Here is a correct example: $exampleJson.", e)
    }
  }

  def parseJson(json: JValue): ResourceInformation = {
    implicit val formats = DefaultFormats
    try {
      json.extract[ResourceInformationJson].toResourceInformation
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n", e)
    }
  }
}

/** A case class to simplify JSON serialization of [[ResourceInformation]]. */
private case class ResourceInformationJson(name: String, addresses: Seq[String]) {

  def toJValue: JValue = {
    Extraction.decompose(this)(DefaultFormats)
  }

  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(name, addresses.toArray)
  }
}
