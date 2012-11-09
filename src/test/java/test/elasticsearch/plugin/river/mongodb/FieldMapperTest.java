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
  public void testNoMappingConfigProvided() {
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
  public void testMappingChangingNames() {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("a", 1);
    source.put("b", "string");
    source.put("c", "security");
    source.put("d", 25);

    FieldMapperConfig config = new FieldMapperConfig();
    config.generate("mapped").from("a","b");
    config.generate("display").from("c");

    Map<String, Object> mapped = new FieldMapper(config).map(source);

    assertEquals(mapped.keySet().size(), 6);
    assertEquals(mapped.get("a"), 1);
    assertEquals(mapped.get("b"), "string");
    assertEquals(mapped.get("c"), "security");
    assertEquals(mapped.get("d"), 25);
    assertEqualsNoOrder((Object[])mapped.get("mapped"), new Object[]{1, "string"});
    assertEquals(mapped.get("display"), "security");

  }

  @Test
  public void testInvalidMapping()  {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("key1", 1);
    source.put("key2", 2);

    FieldMapperConfig config = new FieldMapperConfig();
    config.generate("newField").from("inexistent");
    config.generate("newField2").from("key2");

    Map<String, Object> mapped = new FieldMapper(config).map(source);

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
    config.generate("newField").from("key1","key2");

    Map<String, Object> mapped = new FieldMapper(config).map(source);

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
    config.generate("newField").from("key1","key2");

    Map<String, Object> mapped = new FieldMapper(config).map(source);

    assertEquals(mapped.keySet().size(), 2);
    assertEquals(mapped.get("key2"),2);
    assertEquals(mapped.get("newField"),2);


  }

  @Test
  public void shouldBuildFromConfig()  {
    List<Map<String,Object>> settings = new ArrayList<Map<String, Object>>();

    Map<String,Object> field1 = new HashMap<String, Object>();
    field1.put("generate","suggest");
    field1.put("from", Arrays.asList("field1","field2","field3"));

    Map<String,Object> field2 = new HashMap<String, Object>();
    field2.put("generate","ranking");
    field2.put("from","r");

    settings.add(field1);
    settings.add(field2);

    FieldMapperConfig fieldMapperConfig = new FieldMapperConfig(settings);

    assertEquals(fieldMapperConfig.getMappingForField("r"),"ranking");
    assertEquals(fieldMapperConfig.getMappingForField("field1"),"suggest");
    assertEquals(fieldMapperConfig.getMappingForField("field2"),"suggest");
    assertEquals(fieldMapperConfig.getMappingForField("field3"),"suggest");


  }

}
