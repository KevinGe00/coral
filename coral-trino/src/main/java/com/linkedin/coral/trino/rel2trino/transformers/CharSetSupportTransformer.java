///**
// * Copyright 2023 LinkedIn Corporation. All rights reserved.
// * Licensed under the BSD-2 Clause license.
// * See LICENSE in the project root for license information.
// */
//package com.linkedin.coral.trino.rel2trino.transformers;
//
//import com.linkedin.coral.common.transformers.SqlCallTransformer;
//import java.util.ArrayList;
//import java.util.List;
//import org.apache.calcite.sql.SqlBasicCall;
//import org.apache.calcite.sql.SqlBasicTypeNameSpec;
//import org.apache.calcite.sql.SqlCall;
//import org.apache.calcite.sql.SqlDataTypeSpec;
//import org.apache.calcite.sql.SqlKind;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.SqlNodeList;
//import org.apache.calcite.sql.SqlRowTypeNameSpec;
//import org.apache.calcite.sql.SqlRowTypeSpec;
//import org.apache.calcite.sql.SqlSelect;
//import org.apache.calcite.sql.SqlTypeNameSpec;
//import org.apache.calcite.sql.SqlWindow;
//import org.apache.calcite.sql.fun.SqlCastFunction;
//import org.apache.calcite.sql.parser.SqlParserPos;
//import org.apache.calcite.sql.type.SqlTypeName;
//
//import static org.apache.calcite.rel.rel2sql.SqlImplementor.*;
//
//
///**
// * This class transforms the ordering in the input SqlCall to be compatible with Trino engine.
// * There is no need to override ASC inputs since the default null orderings of Coral IR, Hive and Trino all match.
// * However, "DESC NULLS LAST" need to be overridden to remove the redundant "NULLS LAST" since
// * Trino defaults to a NULLS LAST ordering for DESC anyways.
// *
// * For example, "SELECT * FROM TABLE_NAME ORDER BY COL_NAME DESC NULLS LAST "
// * is transformed to "SELECT * FROM TABLE_NAME ORDER BY COL_NAME DESC"
// *
// * Also, "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC NULLS LAST) AS rid FROM foo"
// * is transformed to "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rid FROM foo"
// */
//public class CharSetSupportTransformer extends SqlCallTransformer {
//  @Override
//  protected boolean condition(SqlCall sqlCall) {
//    return sqlCall.getOperator().kind == SqlKind.CAST && sqlCall.getOperandList().size() > 0;
//  }
//
//  @Override
//  protected SqlCall transform(SqlCall sqlCall) {
//
//    List<SqlNode> operandList = sqlCall.getOperandList();
////    List<SqlNode> newOperands = new ArrayList<>();
//    for (int i = 0; i < operandList.size(); i++) {
//      SqlNode operand = operandList.get(i);
//
//      if (operand instanceof SqlDataTypeSpec) {
//        SqlTypeNameSpec typeNameSpec = ((SqlDataTypeSpec) operand).getTypeNameSpec();
//
//        if (typeNameSpec instanceof SqlRowTypeNameSpec) {
//          SqlRowTypeSpec rowType = (SqlRowTypeSpec) operand;
////          new SqlRowTypeSpec(rowType.g)
//          for (SqlDataTypeSpec rowDataTypeSpec : rowType.getFieldTypeSpecs()) {
//            rowDataTypeSpec.getTypeName()
//          }
//
//        } else if (typeNameSpec instanceof SqlBasicTypeNameSpec) {
//          sqlCall.setOperand(i, new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, ((SqlBasicTypeNameSpec)typeNameSpec).getPrecision(), SqlParserPos.ZERO), SqlParserPos.ZERO));
//        }
//
//      }
//
//
//    }
//
//    return sqlCall;
//  }
//
//}
