/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.rel2trino.transformers;

import java.util.List;

import org.apache.calcite.sql.SqlArrayTypeSpec;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlRowTypeSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.SqlTypeName;

import com.linkedin.coral.common.transformers.SqlCallTransformer;

import static org.apache.calcite.sql.parser.SqlParserPos.*;


/**
 * This class transforms SqlCalls with Charset support on to be compatible Trino engine. Charset support
 * enabled determines whether the dialect supports character set names as part of a data type, for instance
 * VARCHAR(30) CHARACTER SET `ISO-8859-1`.
 *
 * For example, "SELECT CAST(2.3 AS VARCHAR(65535) CHARACTER SET "ISO-8859-1")
 * FROM (VALUES  (0)) AS "t" ("ZERO")"
 * is transformed to "SELECT CAST(2.3 AS VARCHAR(65535)")
 *  * FROM (VALUES  (0)) AS "t" ("ZERO")"
 *
 * Also, "SELECT "if"(FALSE, NULL, CAST(ROW('') AS ROW("a" CHAR(0) CHARACTER SET "ISO-8859-1")))
 * FROM (VALUES  (0)) AS "t" ("ZERO")"
 * is transformed to "SELECT "if"(FALSE, NULL, CAST(ROW('') AS ROW("a" CHAR(0))))
 * FROM (VALUES  (0)) AS "t" ("ZERO")"
 *
 * Also, "SELECT CAST(ROW(ARRAY[CAST('tmp' AS VARCHAR(65535) CHARACTER SET "ISO-8859-1")]) AS ROW("value" ARRAY<VARCHAR(65535) CHARACTER SET "ISO-8859-1">))
 * FROM (VALUES  (0)) AS "t" ("ZERO")"
 * is transformed to "SELECT CAST(ROW(ARRAY[CAST('tmp' AS VARCHAR(65535))]) AS ROW("value" ARRAY<VARCHAR(65535)>))
 * FROM (VALUES  (0)) AS "t" ("ZERO")"
 *
 */

//ARRAY[CAST('tmp' AS VARCHAR(65535))]
public class CharSetSupportTransformer extends SqlCallTransformer {
  @Override
  protected boolean condition(SqlCall sqlCall) {
    return sqlCall.getOperator().kind == SqlKind.CAST && sqlCall.getOperandList().size() >= 2;
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    List<SqlNode> operandList = sqlCall.getOperandList();
    SqlNode targetDataType = operandList.get(1);
    assert targetDataType instanceof SqlDataTypeSpec;

    SqlDataTypeSpec dataTypeSpec = ((SqlDataTypeSpec) targetDataType);
    SqlTypeNameSpec typeNameSpec = dataTypeSpec.getTypeNameSpec();
    String nameSpecType = typeNameSpec.getTypeName().toString();

    if (typeNameSpec instanceof SqlBasicTypeNameSpec && (nameSpecType == "CHAR" || nameSpecType == "VARCHAR")) {
      SqlBasicTypeNameSpec basicTypeNameSpec = ((SqlBasicTypeNameSpec) typeNameSpec);
      sqlCall.setOperand(1,
          createSqlDataTypeSpec(dataTypeSpec, createNoCharSetSqlBasicTypeNameSpec(nameSpecType, basicTypeNameSpec)));
    } else if (typeNameSpec instanceof SqlRowTypeNameSpec) {
      SqlRowTypeSpec rowTypeSpec = (SqlRowTypeSpec) targetDataType;
      List<SqlDataTypeSpec> fieldTypeSpecs = rowTypeSpec.getFieldTypeSpecs();
      for (int j = 0; j < fieldTypeSpecs.size(); j++) {
        SqlDataTypeSpec rowDataTypeSpec = rowTypeSpec.getFieldTypeSpecs().get(j);
        SqlTypeNameSpec rowTypeNameSpec = rowDataTypeSpec.getTypeNameSpec();
        nameSpecType = rowTypeNameSpec.getTypeName().toString();

        if (rowDataTypeSpec instanceof SqlArrayTypeSpec) {
          String elementTypeName = ((SqlArrayTypeSpec) rowDataTypeSpec).getElementTypeSpec().getTypeName().toString();
          if (elementTypeName == "VARCHAR" || elementTypeName == "CHAR") {
            SqlCollectionTypeNameSpec sqlCollectionTypeNameSpec = (SqlCollectionTypeNameSpec) rowTypeNameSpec;
            SqlBasicTypeNameSpec newBasicTypeNameSpec = createNoCharSetSqlBasicTypeNameSpec(elementTypeName,
                (SqlBasicTypeNameSpec) sqlCollectionTypeNameSpec.getElementTypeName());

            fieldTypeSpecs.set(j, createSqlArrayTypeSpec(rowDataTypeSpec, newBasicTypeNameSpec));
          }
        } else if (rowTypeNameSpec instanceof SqlBasicTypeNameSpec
            && (nameSpecType == "CHAR" || nameSpecType == "VARCHAR")) {
          SqlBasicTypeNameSpec basicTypeNameSpec = (SqlBasicTypeNameSpec) rowTypeNameSpec;
          fieldTypeSpecs.set(j, createSqlDataTypeSpec(rowDataTypeSpec,
              createNoCharSetSqlBasicTypeNameSpec(nameSpecType, basicTypeNameSpec)));
        }
      }
    }

    return sqlCall;
  }

  private static SqlDataTypeSpec createSqlDataTypeSpec(SqlDataTypeSpec dataTypeSpec,
      SqlBasicTypeNameSpec newBasicTypeNameSpec) {
    return new SqlDataTypeSpec(newBasicTypeNameSpec, dataTypeSpec.getParserPosition());
  }

  private static SqlArrayTypeSpec createSqlArrayTypeSpec(SqlDataTypeSpec dataTypeSpec,
      SqlBasicTypeNameSpec newBasicTypeNameSpec) {
    return new SqlArrayTypeSpec(createSqlDataTypeSpec(dataTypeSpec, newBasicTypeNameSpec), ZERO);
  }

  private static SqlBasicTypeNameSpec createNoCharSetSqlBasicTypeNameSpec(String nameSpecType,
      SqlBasicTypeNameSpec basicTypeNameSpec) {
    // CharSet is removed here
    SqlTypeName newTypeName = (nameSpecType == "CHAR") ? SqlTypeName.CHAR : SqlTypeName.VARCHAR;
    SqlBasicTypeNameSpec newBasicTypeNameSpec = new SqlBasicTypeNameSpec(newTypeName, basicTypeNameSpec.getPrecision(),
        basicTypeNameSpec.getScale(), basicTypeNameSpec.getParserPos());
    return newBasicTypeNameSpec;
  }

}
