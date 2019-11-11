package com.linkedin.coral.presto.rel2presto.functions;

import com.linkedin.coral.com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;


/**
 * GenericFunctionTemplate is a template UDF abstract class that takes a return type and a function name.
 * This template is used by UDFs in Presto that have dynamic return types and a lambda function parameter.
 * The lambda function parameter will be represented as a string literal input.
 * Since these lambda functions are internally used APIs, we do not set strict validations.
 *
 * The unparsed output of a GenericTemplateFunction would be as follows:
 *   " ... [FUNCTION_NAME]([STRING_PARAMETER]) ... "
 * NOTE: Since the input parameter is a lambda function, we do not want to capture it as a string when being parsed
 * by Presto, so there will be no quotations around the [STRING_PARAMETER]
 *
 * Since the return type is dynamic, derived classes can set an appropriate return type using the genericDataType
 * constructor parameter.
 */
abstract class GenericTemplateFunction extends SqlUserDefinedFunction {

  private final RelDataType genericDataType;

  public GenericTemplateFunction(RelDataType genericDataType, String functionName) {
    super(new SqlIdentifier(functionName, SqlParserPos.ZERO), null, null,
        null, null, null);
    this.genericDataType = genericDataType;
  }

  @Override
  public RelDataType inferReturnType(final SqlOperatorBinding opBinding) {
    return genericDataType;
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return genericDataType;
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    return true;
  }

  @Override
  protected void checkOperandCount(SqlValidator validator, SqlOperandTypeChecker argTypeChecker, SqlCall call) {
    return;
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    Preconditions.checkState(call.operandCount() == 1);
    String lambdaFunctionString = call.operand(0).toString();

    // SqlCharStringLiterals add beginning and trailing single quotation marks when passing in a literal.
    // Since the input parameter is set as a String input, it will also have these quotations.
    // However, we do not want Presto to capture the parameter as a string input, but as a lambda function.
    // We remove the quotation marks here.
    if (lambdaFunctionString.charAt(0) == '\''
        && lambdaFunctionString.charAt(lambdaFunctionString.length() - 1) == '\'') {
      lambdaFunctionString = lambdaFunctionString.substring(1, lambdaFunctionString.length() - 1);
    }

    final SqlWriter.Frame frame = writer.startFunCall(getName());
    writer.literal(lambdaFunctionString);
    writer.endFunCall(frame);
  }
}
