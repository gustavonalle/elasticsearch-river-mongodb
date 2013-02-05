package org.elasticsearch.river.mongodb.util;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldMapperConfig {


  private ListMultimap<String, String> fieldMaps = ArrayListMultimap.create();
  private Map<String, Boolean> keepSourceMap = new HashMap<String, Boolean>();

  private String fieldName;
  private boolean keepOriginal = true;
  private String[] sources;

  public FieldMapperConfig() {
  }

  public FieldMapperConfig(List<Map<String, Object>> config) {
    for (Map<String, Object> fieldConfig : config) {
      String generatedField = fieldConfig.get("generate").toString();
      Object from = fieldConfig.get("from");
      Object keepSource = fieldConfig.get("keep_source");
      if (from instanceof String) {
        fieldMaps.put(from.toString(), generatedField);
      }
      if (from instanceof List) {
        List<String> fieldList = (List<String>) from;
        for (String field : fieldList) {
          fieldMaps.put(field, generatedField);
        }
      }
      if (keepSource != null) {
        keepSourceMap.put(generatedField, Boolean.valueOf(keepSource.toString()));
      }
    }

  }

  public FieldMapperConfig generate(String fieldName) {
    this.fieldName = fieldName;
    return this;
  }

  public FieldMapperConfig keepOriginal(boolean keepOriginal) {
    this.keepOriginal = keepOriginal;
    return this;
  }

  public FieldMapperConfig from(String... sources) {
    this.sources = sources;
    return this;

  }

  public FieldMapperConfig create() {
    if (fieldName == null) {
      throw new IllegalArgumentException("no destination field has been set, call generate(fieldName) first");
    }
    if (sources == null) {
      throw new IllegalArgumentException("source fields cannot be null");
    }
    for (String source : sources) {
      fieldMaps.put(source, fieldName);
      keepSourceMap.put(fieldName, keepOriginal);
    }
    return this;

  }

  public List<String> getMappingForField(String fieldName) {
    return fieldMaps.get(fieldName);
  }

  public ListMultimap<String, String> getAllFieldMappings() {
    return fieldMaps;
  }

  public boolean isKeepOriginal(String fieldName) {
    return keepSourceMap.containsKey(fieldName) && keepSourceMap.get(fieldName);
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    for (Map.Entry<String, String> entry : fieldMaps.entries()) {
      buffer.append(entry.getKey() + "->" + entry.getValue() + "[keep orig.:" + keepSourceMap.get(entry.getValue()) + "],");
    }
    return buffer.toString();
  }
}
