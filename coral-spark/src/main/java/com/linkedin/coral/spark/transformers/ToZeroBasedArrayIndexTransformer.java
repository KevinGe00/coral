/**
 * Copyright 2023-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark.transformers;

import java.math.BigDecimal;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;

import com.linkedin.coral.common.transformers.SqlCallTransformer;
import com.linkedin.coral.common.utils.TypeDerivationUtil;


/**
 * ToZeroBasedArrayIndexTransformer transforms the SqlItemOperator to be compatible with Spark engine.
 * The transformation involves changing the index of the array reference to be zero-based since Coral RelNode stores
 * array indexes with a +1.
 *
 * Example 1:
 *   SELECT array_col[1] FROM table
 *    is translated to
 *   SELECT array_col[0] FROM table
 *
 * Example 2:
 *   SELECT array_col[size(array_col) - 1] FROM table
 *    is translated to
 *   SELECT array_col[size(array_col) + 1 - 1] FROM table
 *    where plus 1 came from Calcite and minus 1 comes from this transformer
 *
 */
public class ToZeroBasedArrayIndexTransformer extends SqlCallTransformer {

  public ToZeroBasedArrayIndexTransformer(TypeDerivationUtil typeDerivationUtil) {
    super(typeDerivationUtil);
  }

  @Override
  protected boolean condition(SqlCall sqlCall) {
    final SqlOperator operator = sqlCall.getOperator();
    final String operatorName = operator.getName();
    // Alternative check for SqlItemOperator since SqlItemOperator cannot be accessed from outside package
    return operator.getKind() == SqlKind.OTHER_FUNCTION && operatorName.equals("ITEM");
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    SqlNode columnRef = sqlCall.operand(0);
    SqlNode itemRef = sqlCall.operand(1);
    if (deriveRelDatatype(columnRef) instanceof ArraySqlType) {
      SqlNode newItemRef;
      if (itemRef instanceof SqlNumericLiteral) {
        final Integer oldIndexVal = ((SqlNumericLiteral) itemRef).getValueAs(Integer.class);
        newItemRef = SqlNumericLiteral.createExactNumeric(new BigDecimal(oldIndexVal - 1).toString(),
            itemRef.getParserPosition());
      } else {
        // Example of item reference not being a numeric literal is "array_col[size(array_col) - 1]"
        newItemRef = SqlStdOperatorTable.MINUS.createCall(itemRef.getParserPosition(), itemRef,
            SqlNumericLiteral.createExactNumeric("1", itemRef.getParserPosition()));
      }
      return sqlCall.getOperator().createCall(sqlCall.getParserPosition(), columnRef, newItemRef);
    }

    return sqlCall;
  }
}
