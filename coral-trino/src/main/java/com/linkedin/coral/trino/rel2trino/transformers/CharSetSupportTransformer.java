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
 * This class transforms the ordering in the input SqlCall to be compatible with Trino engine.
 * There is no need to override ASC inputs since the default null orderings of Coral IR, Hive and Trino all match.
 * However, "DESC NULLS LAST" need to be overridden to remove the redundant "NULLS LAST" since
 * Trino defaults to a NULLS LAST ordering for DESC anyways.
 *
 * For example, "SELECT * FROM TABLE_NAME ORDER BY COL_NAME DESC NULLS LAST "
 * is transformed to "SELECT * FROM TABLE_NAME ORDER BY COL_NAME DESC"
 *
 * Also, "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC NULLS LAST) AS rid FROM foo"
 * is transformed to "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rid FROM foo"
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
