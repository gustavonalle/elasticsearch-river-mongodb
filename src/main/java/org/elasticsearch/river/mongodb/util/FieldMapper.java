package org.elasticsearch.river.mongodb.util;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FieldMapper {

  private final FieldMapperConfig config;

  public FieldMapper(FieldMapperConfig config) {
    this.config = config;
  }


  public Map<String, Object> map(Map<String, Object> source) {
    HashMap<String, Object> mapped = new HashMap<String, Object>();
    for (Map.Entry<String, Object> entry : source.entrySet()) {
      String destination = config.getMappingForField(entry.getKey());
      if (destination == null) {
        mapped.put(entry.getKey(), entry.getValue());
      } else {
        mapped.put(destination, combine(mapped.get(destination), entry.getValue()));
      }
    }
    return mapped;

  }

  private Object combine(Object existing, Object newValue) {
    if (existing == null) return newValue;
    if (newValue == null) return existing;
    if (existing instanceof Object[]) {
      Object[] array = (Object[]) existing;
      int length = array.length;
      Object[] newArray = Arrays.copyOf(array, length + 1);
      newArray[length] = newValue;
      return newArray;
    } else {
      return new Object[]{existing, newValue};
    }
  }

}
