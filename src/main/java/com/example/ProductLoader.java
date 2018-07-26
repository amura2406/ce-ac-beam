/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProductLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ProductLoader.class);

  static class Product {
    private Long sku;
    private String name;
    private String image;

    public Long getSku() {
      return sku;
    }

    public void setSku(Long sku) {
      this.sku = sku;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getImage() {
      return image;
    }

    public void setImage(String image) {
      this.image = image;
    }
  }

  static class NormalizeJson extends SimpleFunction<String, String> {
    @Override
    public String apply(String input) {
      String result = input.trim();
      if (result.startsWith("[")) {
        result = result.substring(1);
      }
      if (result.endsWith("]")) {
        result = result.substring(0, result.length()-1);
      }
      if (result.endsWith(",")) {
        result = result.substring(0, result.length()-1);
      }

      ObjectMapper mpr = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      try {
        LOG.info("LINE:  " + result);
        Product p = mpr.readValue(result, Product.class);
        result = mpr.writer().writeValueAsString(p);
      } catch (Exception e) {
        LOG.error("Error while JSON marshalling", e);
        return "";
      }
      return result;
    }
  }

  public interface ProductLoaderOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("gs://gcp-ce-product/*.json")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of GCP PubSub topic")
    @Validation.Required
    String getOutputTopic();
    void setOutputTopic(String value);
  }

  public static void main(String[] args) {
    ProductLoaderOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(ProductLoaderOptions.class);

    Pipeline p = Pipeline.create(options);


    p.apply(TextIO.read().from(options.getInputFile()))
        .apply(MapElements.via(new NormalizeJson()))
        .apply(PubsubIO.writeStrings().to(options.getOutputTopic()));

    p.run().waitUntilFinish();
  }
}
