package com.linkedin.coral.hive.hive2rel.functions;

import com.google.common.base.Preconditions;
import com.linkedin.coral.com.google.common.collect.ImmutableList;
import com.linkedin.coral.hive.hive2rel.HiveTable;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.hadoop.hive.metastore.api.Table;

import static com.google.common.base.Preconditions.*;
import static org.apache.calcite.sql.parser.SqlParserPos.*;


/**
 * Class to resolve hive function names in SQL to HiveFunction.
 */
public class HiveFunctionResolver {

  public final HiveFunctionRegistry registry;

  public HiveFunctionResolver(HiveFunctionRegistry registry) {
    this.registry = registry;
  }

  /**
   * Resolves hive function name to specific HiveFunction. This method
   * first attempts to resolve function by its name. If there is no match,
   * this attempts to match dali-style function names (DB_TABLE_VERSION_FUNCTION).
   * Right now, this method does not validate parameters leaving it to
   * the subsequent validator and analyzer phases to validate parameter types.
   * @param functionName hive function name
   * @param isCaseSensitive is function name case-sensitive
   * @param hiveTable handle to Hive table representing metastore information. This is used for resolving
   *                  Dali function names, which are resolved using table parameters
   * @return resolved hive functions
   * @throws UnknownSqlFunctionException if the function name can not be resolved.
   */
  public HiveFunction tryResolve(@Nonnull String functionName, boolean isCaseSensitive, @Nullable Table hiveTable) {
    checkNotNull(functionName);
    Collection<HiveFunction> functions = registry.lookup(functionName, isCaseSensitive);
    if (functions.isEmpty() && hiveTable != null) {
      functions = tryResolveAsDaliFunction(functionName, hiveTable);
    }
    if (functions.isEmpty()) {
      throw new UnknownSqlFunctionException(functionName);
    }
    if (functions.size() == 1) {
      return functions.iterator().next();
    }
    // we've overloaded function names. Calcite will resolve overload later during semantic analysis.
    // For now, create a placeholder SqlNode for the function. We want to use Dali class name as function
    // name if this is overloaded function name.
    return unresolvedFunction(functions.iterator().next().getSqlOperator().getName(), hiveTable);
  }

  /**
   * Resolves function to concrete operator.
   * @param functionName function name to resolve
   * @param isCaseSensitive is the function name case-sensitive
   * @return list of matching HiveFunctions or empty list if there is no match
   */
  public Collection<HiveFunction> resolve(String functionName, boolean isCaseSensitive) {
    return registry.lookup(functionName, isCaseSensitive);
  }

  /**
   * Tries to resolve function name as Dali function name using the provided Hive table catalog information.
   * This uses table parameters 'function' property to resolve the function name to the implementing class.
   * @param functionName function name to resolve
   * @param table Hive metastore table handle
   * @return list of matching HiveFunctions or empty list if the function name is not in the
   *   dali function name format of db_tableName_functionName
   * @throws UnknownSqlFunctionException if the function name is in Dali function name format but there is no mapping
   */
  public Collection<HiveFunction> tryResolveAsDaliFunction(String functionName, @Nonnull Table table) {
    Preconditions.checkNotNull(table);
    String functionPrefix = String.format("%s_%s_", table.getDbName(), table.getTableName());
    if (!functionName.toLowerCase().startsWith(functionPrefix.toLowerCase())) {
      // Don't throw UnknownSqlFunctionException here because this is not a dali function
      // and this method is trying to resolve only Dali functions
      return ImmutableList.of();
    }
    String funcBaseName = functionName.substring(functionPrefix.length());
    HiveTable hiveTable = new HiveTable(table);
    Map<String, String> functionParams = hiveTable.getDaliFunctionParams();
    String funcClassName = functionParams.get(funcBaseName);
    if (funcClassName == null) {
      throw new UnknownSqlFunctionException(funcClassName);
    }
    return registry.lookup(funcClassName, true);
  }

  private @Nonnull HiveFunction unresolvedFunction(String functionName, Table table) {
    SqlIdentifier funcIdentifier = createFunctionIdentifier(functionName, table);
    return new HiveFunction(functionName,
        new SqlUnresolvedFunction(funcIdentifier, null, null,
            null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION));
  }

  private @Nonnull SqlIdentifier createFunctionIdentifier(String functionName, @Nullable Table table) {
    if (table == null) {
      return new SqlIdentifier(ImmutableList.of(functionName), ZERO);
    } else {
      return new SqlFunctionIdentifier(functionName, ImmutableList.of(table.getDbName(), table.getTableName()));
    }
  }
}