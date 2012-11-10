package test.elasticsearch.plugin.river.mongodb;


import org.elasticsearch.river.mongodb.util.FieldMapper;
import org.elasticsearch.river.mongodb.util.FieldMapperConfig;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;

public class FieldMapperTest {

  @Test
  public void shouldNotAffectSourceOnEmptyConfig() {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("ca", Integer.valueOf(15));
    source.put("upl", "Silver");
    source.put("acl", "Commodity Spread");

    FieldMapperConfig config = new FieldMapperConfig();

    Map<String, Object> mapped = new FieldMapper(config).map(source);

    assertEquals(mapped.get("ca"), Integer.valueOf(15));
    assertEquals(mapped.get("upl"), "Silver");
    assertEquals(mapped.keySet().size(), 3);

  }


  @Test
  public void shouldKeepOriginalFieldWhenRequested() {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("field1", 1);
    source.put("field2", "value field 2");
    source.put("field3", 3.0);
    source.put("field4", new Date(444444));

    FieldMapperConfig config = new FieldMapperConfig();
    config.generate("123combined").from("field1", "field2", "field3").keepOriginal(true).create();
    config.generate("4renamed").from("field4").keepOriginal(false).create();

    Map<String, Object> mapped = new FieldMapper(config.create()).map(source);

    assertEquals(mapped.keySet().size(), 5);
    assertEquals(mapped.get("field1"), 1);
    assertEquals(mapped.get("field2"), "value field 2");
    assertEquals(mapped.get("field3"), 3.0);
    assertEqualsNoOrder((Object[]) mapped.get("123combined"), new Object[]{1, "value field 2", 3.0});
    assertEquals(mapped.get("4renamed"), new Date(444444));

  }

  @Test
  public void testInvalidMapping() {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("key1", 1);
    source.put("key2", 2);

    FieldMapperConfig config = new FieldMapperConfig();
    config.generate("newField").from("inexistent");
    config.generate("newField2").from("key2");

    Map<String, Object> mapped = new FieldMapper(config.create()).map(source);

    assertEquals(mapped.keySet().size(), 3);
    assertNull(mapped.get("newField"));
    assertEquals(mapped.get("key1"), 1);
    assertEquals(mapped.get("key2"), 2);
    assertEquals(mapped.get("newField2"), 2);

  }


  @Test
  public void shouldIgnoreNullValuesInSource() throws Exception {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("key1", null);
    source.put("key2", 2);

    FieldMapperConfig config = new FieldMapperConfig();
    config.generate("newField").from("key1", "key2");

    Map<String, Object> mapped = new FieldMapper(config.create()).map(source);

    assertEquals(mapped.keySet().size(), 2);
    assertEquals(mapped.get("key2"), 2);
    assertEquals(mapped.get("newField"), 2);


  }

  @Test
  public void shouldIgnoreEmptyValuesInSource() throws Exception {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("key1", "");
    source.put("key2", 2);

    FieldMapperConfig config = new FieldMapperConfig();
    config.generate("newField").from("key1", "key2");

    Map<String, Object> mapped = new FieldMapper(config.create()).map(source);

    assertEquals(mapped.keySet().size(), 2);
    assertEquals(mapped.get("key2"), 2);
    assertEquals(mapped.get("newField"), 2);


  }

  @Test
  public void shouldBuildFromConfig() {
    List<Map<String, Object>> settings = new ArrayList<Map<String, Object>>();

    Map<String, Object> field1 = new HashMap<String, Object>();
    field1.put("generate", "suggest");
    field1.put("from", Arrays.asList("field1", "field2", "field3"));
    field1.put("keep_source", false);

    Map<String, Object> field2 = new HashMap<String, Object>();
    field2.put("generate", "ranking");
    field2.put("from", "r");
    field2.put("keep_source", true);

    settings.add(field1);
    settings.add(field2);

    FieldMapperConfig fieldMapperConfig = new FieldMapperConfig(settings);

    assertEquals(fieldMapperConfig.getMappingForField("r"), "ranking");
    assertEquals(fieldMapperConfig.getMappingForField("field1"), "suggest");
    assertEquals(fieldMapperConfig.getMappingForField("field2"), "suggest");
    assertEquals(fieldMapperConfig.getMappingForField("field3"), "suggest");
    assertEquals(fieldMapperConfig.isKeepOriginal("generate"), false);
    assertEquals(fieldMapperConfig.isKeepOriginal("ranking"), true);


  }

}
