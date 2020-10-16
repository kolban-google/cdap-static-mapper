package com.examples.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.StructuredRecord.Builder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

/**
 * Transform that performs mapping from the value in a field to a replacement value by using
 * the original value as a key and selecting the key's value as the new field value.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("StaticMapper")
@Description("Map from a field's value to a new value by performing a lookup/mapping of static data.")
public class StaticMapperTransform extends Transform<StructuredRecord, StructuredRecord> {
  private final Conf config;
  private ArrayList<MappingEntry> mappingEntries;
  private Schema outputSchema;
  /**
   * An entry for mapping which maps the VALUE of a field to a new value by looking up
   * a lookup table keyed by the value.
   */
  private class MappingEntry {
    private String fieldName; // The name of the field in the incoming data that will be mapped.
    private HashMap<String, String> map; // The values for this particular lookup mapping.
    private String defaultValue;


    public MappingEntry(String fieldName, HashMap<String, String> map, String defaultValue) {
      this.fieldName = fieldName;
      this.map = map;
      this.defaultValue = defaultValue;
    } // MappingEntry

    public void doMap(StructuredRecord record, StructuredRecord.Builder builder) {
      System.out.println("MappingEntry::doMap: fieldName=" + fieldName);
      String lookupKey = record.get(fieldName);
      String lookupValue = map.get(lookupKey);
      if (lookupValue == null) {
        lookupValue = defaultValue;
      }
      if (lookupValue == null) {
        System.out.println("Unable to map field " + fieldName);
        return;
      }
      builder.set(fieldName, lookupValue);
    } // doMap
  } // MappingEntry


  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    // nullable means this property is optional
    @Nullable
    @Name("json_data")
    @Description("The JSON data that contains the mappings.")
    private String json_data;

    @Nullable
    @Name("mappings")
    @Description("The mappings from source to target.  The format is field name, JSON key field, JSON value field, default value.")
    private String mappings;
  }

  public StaticMapperTransform(Conf config) {
    this.config = config;
  }

  // configurePipeline is called only once, when the pipeline is deployed. Static validation should be done here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    System.out.println("StaticMapperTransform::configurePipeline");
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    // the output schema is always the same as the input schema
    Schema inputSchema = stageConfigurer.getInputSchema();

    stageConfigurer.setOutputSchema(inputSchema);
  }

  // initialize is called once at the start of each pipeline run
  @Override
  public void initialize(TransformContext context) throws Exception {
    System.out.println("StaticMapperTransform::initialize");

   
    outputSchema = context.getOutputSchema(); // Save the output schema that will be used in the transform() function.
    mappingEntries = new ArrayList<>();

    JsonParser jsonParser = new JsonParser();
    JsonElement jsonDoc = jsonParser.parse(config.json_data);
    if (!jsonDoc.isJsonArray()) {
      System.out.println("NOT AN ARRAY!");
      return;
    }
    JsonArray ja = jsonDoc.getAsJsonArray();

    // Parse the mappings configuration.

    
    List<Map<String, String>> entries = CDAPUtils.parseDelimiters(Arrays.asList("field", "key", "value", "default"), config.mappings);
    for (Map<String, String> entry : entries) {

      String fieldName = entry.get("field");
      String keyName   = entry.get("key");
      String valueName = entry.get("value");
      String defaultValue = entry.get("default");

      if (fieldName == null) {
        System.out.println("Missing field entry");
        continue;
      }
      if (keyName == null) {
        System.out.println("Missing key entry");
        continue;
      }
      if (valueName == null) {
        System.out.println("Missing value entry");
        continue;
      }

      // Iterate over each of the entries in the JSON Array which will be objects. For
      // each object, get the
      // key and value and add them to the map.
      HashMap<String, String> map = new HashMap<>();
      Iterator<JsonElement> it = ja.iterator();
      while (it.hasNext()) {
        JsonElement el = it.next();
        if (el.isJsonObject()) {
          JsonObject jo = el.getAsJsonObject();
          String key   = jo.get(keyName).getAsJsonPrimitive().getAsString();
          String value = jo.get(valueName).getAsJsonPrimitive().getAsString();
          map.put(key, value);
        }
      }

      mappingEntries.add(new MappingEntry(fieldName, map, defaultValue));
    }
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    System.out.println("StaticMapperTransform::transform");
    Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      builder.set(field.getName(), record.get(field.getName()));
    }

    for (MappingEntry entry: mappingEntries) {
      entry.doMap(record, builder);
    }
    emitter.emit(builder.build());
  }
}
