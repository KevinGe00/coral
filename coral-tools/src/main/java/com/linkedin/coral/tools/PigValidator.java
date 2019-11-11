package com.linkedin.coral.tools;

import com.linkedin.coral.hive.hive2rel.HiveMetastoreClient;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.pig.rel2pig.PigLoadFunction;
import com.linkedin.coral.pig.rel2pig.RelToPigLatinConverter;
import com.linkedin.coral.pig.rel2pig.TableToPigPathFunction;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * The PigValidator validates the following for a given DaliView:
 *   - The DaliView can be translated by Coral to Pig Latin
 *   - The DaliView translation can be parsed by the Pig Engine
 *   - The DaliView translation produces the same schema as the legacy reader.
 */
public class PigValidator implements LanguageValidator {

  private static final Log LOG = LogFactory.getLog("PigValidator");
  private static final String REGISTER_DALI_PIG_JAR_STATEMENT =
      "REGISTER '/export/apps/dali/pig/dali-data-pig-9-all.jar';";
  private static final String LOAD_DALI_VIEW_TEMPLATE =
      "%s = LOAD 'dalids:///%s.%s' USING dali.data.pig.DaliStorage();";
  private static final String DESCRIBE_TEMPLATE = "DESCRIBE %s;";
  private static final String OUTPUT_RELATION_TEMPLATE = "%s_%s_view_definition";

  private static final String EXECUTE_PIG_SCRIPT_COMMAND = "/export/apps/pig/stable/bin/pig -f %s";

  public PigValidator() {

  }

  @Override
  public String getCamelName() {
    return "pigLatin";
  }

  @Override
  public String getStandardName() {
    return "Pig Latin";
  }

  @Override
  public void convertAndValidate(String db, String table, HiveMetastoreClient hiveMetastoreClient,
      PrintWriter outputWriter) {

    try {
      final String outputRelation = String.format(OUTPUT_RELATION_TEMPLATE, db, table);

      final String coralPigLatin = getCoralBasedDaliViewPigLatin(db, table, hiveMetastoreClient, outputRelation);
      final String coralSchema = getSchemaString(coralPigLatin, outputRelation);

      final String legacyPigLatin = getLegacyDaliViewPigLatin(db, table, outputRelation);
      final String legacySchema = getSchemaString(legacyPigLatin, outputRelation);

      if (!schemaMatches(coralSchema, legacySchema)) {
        throw new RuntimeException(String.format(
            "Mismatched schemas for %s.%s with translated query:\n%s\nCORAL-BASED DALIREADER SCHEMA:\n%s\nLEGACY HIVE-BASED DALIREADER SCHEMA:\n%s",
            db, table, coralPigLatin, coralSchema, legacySchema));
      }

      outputWriter.write(coralPigLatin);
      outputWriter.flush();

    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Translating DaliView %s.%s failed with:\n%s", db, table, e.getMessage()));
    }
  }

  /**
   * Coral-based Pig Latin translation that natively queries the records of a given DaliView.
   *
   * @param db A database.
   * @param table A table in the given db.
   * @param hiveMetastoreClient HiveMetastoreClient that has an already established connection.
   * @param outputRelation The alias where the DaliView is stored.
   * @return Pig Latin generated by Coral to query a DaliView from its base tables.
   */
  private String getCoralBasedDaliViewPigLatin(String db, String table, HiveMetastoreClient hiveMetastoreClient,
      String outputRelation) {
    final PigLoadFunction pigLoadFunction = (String d, String t) -> "dali.data.pig.DaliStorage()";
    final TableToPigPathFunction tableToPigPathFunction =
        (String d, String t) -> String.format("dalids:///%s.%s", d, t);

    final HiveToRelConverter hiveToRelConverter = HiveToRelConverter.create(hiveMetastoreClient);
    final RelNode rel = hiveToRelConverter.convertView(db, table);

    final RelToPigLatinConverter converter = new RelToPigLatinConverter(pigLoadFunction, tableToPigPathFunction);

    return converter.convert(rel, outputRelation);
  }

  /**
   * Pig Latin to query a DaliView using the legacy DaliStorage reader.
   *
   * @param db A database.
   * @param table A table in the given db.
   * @param outputRelation The alias where the DaliView is stored.
   * @return Pig Latin to query a DaliView using legacy DaliStorage reader.
   */
  private String getLegacyDaliViewPigLatin(String db, String table, String outputRelation) {
    return String.format(LOAD_DALI_VIEW_TEMPLATE, outputRelation, db, table);
  }

  /**
   * Generates a Pig Engine Schema String of the output of the given Pig Latin statements.
   *
   * @param pigLatin Pig Latin to be executed.
   * @param outputRelation The alias where the DaliView is stored.
   * @return Pig Engine String representation of the schema generated by the out of the given pigLatin.
   * @throws IOException if the retrieval of the schema of the Pig Latin fails.
   */
  private String getSchemaString(String pigLatin, String outputRelation) throws IOException {

    // The schema of the given pigLatin is given by the following:
    // 1. Create a temp file that stores the Pig Latin to compute the view from the given pigLatin.
    // 2. Create a process that executes the temp file through the Pig engine using:
    //      'pig -f [tmp_file].pig'
    // 3. Read the standard output of the pig process and retrieve the schema from the logs.
    // 4. Return the schema.

    final String describePigLatin = String.join("\n",
        REGISTER_DALI_PIG_JAR_STATEMENT, pigLatin, String.format(DESCRIBE_TEMPLATE, outputRelation));

    final File tempScriptFile = File.createTempFile("coral-pig-test-script", ".pig").getAbsoluteFile();
    tempScriptFile.deleteOnExit();

    Files.write(tempScriptFile.toPath(), Arrays.asList(describePigLatin.split("\n")));

    final ProcessBuilder processBuilder = new ProcessBuilder("/bin/bash", "-c",
        String.format(EXECUTE_PIG_SCRIPT_COMMAND, tempScriptFile.getAbsolutePath()));

    final Process process = processBuilder.start();

    try {
      process.waitFor();
    } catch (Exception e) {
      LOG.error(e.toString());
    }

    return Arrays.asList(IOUtils.toString(process.getInputStream()).split("\n")).stream()
        .map(String::trim)
        .filter(x -> x.contains(outputRelation))
        .collect(Collectors.joining());
  }

  /**
   * Returns true if and only if at least one of these conditions are true:
   *   - coralSchema is exactly equivalent to legacySchema
   * Otherwise, return false.
   *
   * TODO(ralam): Add a condition that returns true if:
   *   - coralSchema contains a superset of of all top-level and nested fields in legacySchema.
   *
   * @param coralSchema The schema of the output given by Coral translation.
   * @param legacySchema The schema of the output of the legacy reader.
   * @return True if coralSchema is strictly equivalent to legacySchema. Otherwise, return false.
   */
  private boolean schemaMatches(String coralSchema, String legacySchema) {
    return coralSchema.equals(legacySchema);
  }

}