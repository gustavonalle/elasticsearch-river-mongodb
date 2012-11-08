package org.elasticsearch.river.mongodb.util;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldMapperConfig {

  private Map<String, String> fieldMaps = new HashMap<String, String>();

  private String fieldName;

  public FieldMapperConfig() {}

  public FieldMapperConfig(List<Map<String,Object>> config) {
    for(Map<String,Object> fieldConfig : config) {
      String generatedField = fieldConfig.get("generate").toString();
      Object from = fieldConfig.get("from");
      if(from instanceof String) {
        fieldMaps.put(from.toString(),generatedField);
      }
      if (from instanceof List) {
        List<String> fieldList = (List<String>) from;
        for(String field:fieldList) {
           fieldMaps.put(field,generatedField);
        }
      }
    }

  }

  public FieldMapperConfig generate(String fieldName) {
    this.fieldName = fieldName;
    return this;
  }

  public FieldMapperConfig from(String... sources) {
    if (fieldName == null) {
      throw new IllegalArgumentException("no destination field has been set, call generate(fieldName) first");
    }
    if (sources == null) {
      throw new IllegalArgumentException("source fields cannot be null");
    }
    for (String source : sources) {
      fieldMaps.put(source, fieldName);
    }
    return this;
  }

  public String getMappingForField(String fieldName) {
    return fieldMaps.get(fieldName);
  }
}
