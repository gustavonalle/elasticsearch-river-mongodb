package org.elasticsearch.river.mongodb.util;


import com.google.common.collect.ListMultimap;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.*;

public class FieldMapper {

  private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

  private final FieldMapperConfig config;

  public FieldMapper(FieldMapperConfig config) {
    this.config = config;
  }

  public Map<String, Object> map(Map<String, Object> source) {
    Map sourceCopy = new HashMap(source);
    ListMultimap<String, String> fieldMaps = config.getAllFieldMappings();
    JexlEngine jexlEngine = new JexlEngine();
    MapContext mapContext = new MapContext(sourceCopy);
    HashMap<String, Object> mapped = new HashMap<String, Object>(sourceCopy);
    for (String from : fieldMaps.asMap().keySet()) {
      List<String> to = fieldMaps.get(from);
      Expression expression = jexlEngine.createExpression(from);
      Object newValue = expression.evaluate(mapContext);
      for (String toField : to) {
        Object existingValue = mapped.get(toField);
        mapped.put(toField, combine(existingValue, newValue));
        if (!config.isKeepOriginal(toField)) {
          mapped.remove(from);
        }
      }
    }
    logger.info("Mapped from {}, to {}", sourceCopy, mapped);
    return mapped;

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

  @Override
  public String toString() {
    return config.toString();
  }
}
