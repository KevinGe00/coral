/**
 * Copyright 2022-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.entity;

import com.linkedin.coral.coralservice.utils.RewriteType;


public class VisualizationRequestBody {
  private String sourceLanguage;
  private String query;

  private RewriteType rewriteType;

  public String getSourceLanguage() {
    return sourceLanguage;
  }

  public String getQuery() {
    return query;
  }

  public RewriteType getRewriteType() {
    return rewriteType;
  }
}
