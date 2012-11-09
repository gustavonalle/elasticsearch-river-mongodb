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
    removeNullsAndEmpties(source);
    HashMap<String, Object> mapped = new HashMap<String, Object>(source);
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


  private void removeNullsAndEmpties(Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() == null || entry.getValue().toString().isEmpty()) {
        map.remove(entry.getKey());
      }
    }
  }

  private Object combine(Object existing, Object newValue) {
    if (existing == null || existing.toString().isEmpty()) return newValue;
    if (newValue == null || newValue.toString().isEmpty()) return existing;
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
