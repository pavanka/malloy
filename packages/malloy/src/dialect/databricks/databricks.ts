/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
import {
  DateUnit,
  Expr,
  ExtractUnit,
  Sampling,
  TimeFieldType,
  TimeValue,
  TimestampUnit,
  isSamplingEnable,
  isSamplingPercent,
  isSamplingRows,
  mkExpr,
  TypecastFragment,
  FieldAtomicTypeDef,
} from '../../model/malloy_types';
import {DATABRICKS_FUNCTIONS} from './functions';
import {DialectFunctionOverloadDef} from '../functions';
import {
  Dialect,
  DialectFieldList,
  OrderByClauseType,
  QueryInfo,
  dayIndex,
  allUnits,
  qtz,
} from '../dialect';

// https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
const databricksToMalloyTypes: {
  [key: string]: FieldAtomicTypeDef & {raw_type: string};
} = {
  'boolean': {type: 'boolean', raw_type: 'boolean'},
  'tinyint': {type: 'number', numberType: 'integer', raw_type: 'tinyint'},
  'smallint': {type: 'number', numberType: 'integer', raw_type: 'smallint'},
  'int': {type: 'number', numberType: 'integer', raw_type: 'int'},
  'bigint': {type: 'number', numberType: 'integer', raw_type: 'bigint'},
  'float': {type: 'number', numberType: 'float', raw_type: 'float'},
  'double': {type: 'number', numberType: 'float', raw_type: 'double'},
  'decimal': {type: 'number', numberType: 'float', raw_type: 'decimal'},
  'char': {type: 'string', raw_type: 'string'},
  'string': {type: 'string', raw_type: 'string'},
  'varchar': {type: 'string', raw_type: 'string'},
  'binary': {type: 'string', raw_type: 'binary'},
  'date': {type: 'date', raw_type: 'date'},
  'timestamp': {type: 'timestamp', raw_type: 'timestamp'},
  // Uncomment and modify the following lines if needed
  // 'array': {type: 'unsupported', raw_type: 'array'},
  // 'struct': {type: 'unsupported', raw_type: 'struct'},
  // 'map': {type: 'unsupported', raw_type: 'map'},
  // 'union': {type: 'unsupported', raw_type: 'union'},
  // 'udt': {type: 'unsupported', raw_type: 'udt'},
  // 'null': {type: 'unsupported', raw_type: 'null'},
  // 'interval_day_time': {type: 'unsupported', raw_type: 'interval_day_time'},
  // 'interval_year_month': {type: 'unsupported', raw_type: 'interval_year_month'},
};

const extractionMap: Record<string, string> = {
  'day_of_week': 'dow',
  'day_of_year': 'doy',
};

export class DatabricksDialect extends Dialect {
  name = 'databricks';
  experimental = true;
  defaultNumberType = 'DECIMAL';
  defaultDecimalType = 'DECIMAL';
  udfPrefix = '__udf';
  hasFinalStage = false;
  divisionIsInteger = false;
  supportsSumDistinctFunction = false;
  unnestWithNumbers = false;
  defaultSampling = {rows: 50000};
  supportUnnestArrayAgg = false;
  supportsAggDistinct = false;
  supportsCTEinCoorelatedSubQueries = false;
  dontUnionIndex = true;
  supportsQualify = false;
  supportsSafeCast = false;
  supportsNesting = true;
  cantPartitionWindowFunctionsOnExpressions = false;
  orderByClause: OrderByClauseType = 'output_name';
  nullMatchesFunctionSignature = false;
  supportsSelectReplace = false;
  supportsComplexFilteredSources = false;
  supportsTempTables = false;

  sqlTypeToMalloyType(sqlType: string): FieldAtomicTypeDef | undefined {
    return databricksToMalloyTypes[sqlType.toLowerCase()];
  }

  malloyTypeToSQLType(malloyType: FieldAtomicTypeDef): string {
    if (malloyType.type === 'number') {
      if (malloyType.numberType === 'integer') {
        return 'integer';
      } else if (malloyType.numberType === 'float') {
        return 'float';
      } else if (malloyType.numberType === 'decimal') {
        return 'decimal';
      } else {
        return 'double';
      }
    } else if (malloyType.type === 'string') {
      return 'string';
    }
    return malloyType.type;
  }

  quoteTablePath(tablePath: string): string {
    return tablePath;
  }

  sqlGroupSetTable(groupSetCount: number): string {
    // return `CROSS JOIN (SELECT group_set FROM (SELECT sequence(0, ${
    //   groupSetCount
    // }) as series) lateral view explode(series) as group_set)`;
    return `CROSS JOIN (SELECT explode(sequence(0, ${groupSetCount})) AS group_set)`;
  }

  sqlAnyValue(groupSet: number, fieldName: string): string {
    return `ANY_VALUE(CASE WHEN group_set=${groupSet} THEN ${fieldName} END) IGNORE NULLS`;
  }

  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    _orderBy: string | undefined,
    limit: number | undefined
  ): string {
    // FIXME (pavanka):
    // 1) orderBy is not directly supported
    // 2) for completeness: may need to double check functions
    // 3) may also need to add more tests and ensure e2e working of most tests too.

    // slice(collect_list(struct(p_name as a, p_type as b)), 1, 1)
    const fields = fieldList
      .map(f => `\n ${f.sqlExpression} AS ${f.sqlOutputName}`)
      .join(', ');
    let ret = `COLLECT_LIST(STRUCT(${fields})) FILTER (WHERE group_set=${groupSet})`;
    if (limit !== undefined) {
      ret = `SLICE(${ret}, 1, ${limit})`;
    }
    return ret;
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    const fields = fieldList
      .map(f => `${f.sqlExpression} AS ${f.sqlOutputName}`)
      .join(', ');
    return `ANY_VALUE(CASE WHEN group_set=${groupSet} THEN STRUCT(${fields}))`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `MAX(CASE WHEN group_set=${groupSet} THEN ${name} END) as ${sqlName}`;
  }

  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const fields = fieldList
      .map(f => `${f.sqlExpression} AS ${f.sqlOutputName}`)
      .join(', ');
    const nullValues = fieldList
      .map(f => `NULL AS ${f.sqlOutputName}`)
      .join(', ');

    return `COALESCE(ANY_VALUE(CASE WHEN group_set=${groupSet} THEN STRUCT(${fields})), STRUCT(${nullValues}))`;
  }

  sqlGenerateUUID(): string {
    return 'UUID()';
  }

  sqlMaybeQuoteIdentifier(identifier: string): string {
    return '`' + identifier + '`';
  }

  sqlCreateTableAsSelect(_tableName: string, _sql: string): string {
    throw new Error('Not implemented Yet');
  }

  sqlNow(): Expr {
    return mkExpr`current_timestamp`;
  }

  sqlRegexpMatch(expr: Expr, regexp: Expr): Expr {
    return mkExpr`regexp_like(${expr}, ${regexp})`;
  }

  sqlLiteralString(literal: string): string {
    return "'" + literal.replace(/'/g, "\\'") + "'";
  }

  sqlLiteralRegexp(literal: string): string {
    return "'" + literal.replace(/'/g, "\\'") + "'";
  }

  sqlLiteralNumber(literal: string): string {
    return literal.includes('.')
      ? `${literal}::${this.defaultNumberType}`
      : literal;
  }

  sqlOrderBy(orderTerms: string[]): string {
    return `ORDER BY ${orderTerms.map(t => `${t} NULLS LAST`).join(',')}`;
  }

  getGlobalFunctionDef(name: string): DialectFunctionOverloadDef[] | undefined {
    return DATABRICKS_FUNCTIONS.get(name);
  }

  sqlCreateFunction(_id: string, _funcText: string): string {
    throw new Error('not implemented yet');
  }

  sqlCreateFunctionCombineLastStage(_lastStageName: string): string {
    throw new Error('not implemented yet');
  }

  sqlDateToString(sqlDateExp: string): string {
    return `(${sqlDateExp})::date::string`;
  }

  castToString(expression: string): string {
    return `CAST(${expression} as STRING)`;
  }

  validateTypeName(sqlType: string): boolean {
    // Letters:              BIGINT
    // Numbers:              INT8
    // Spaces:               TIMESTAMP WITH TIME ZONE
    // Parentheses, Commas:  DECIMAL(1, 1)
    // Brackets:             INT[ ]
    return sqlType.match(/^[A-Za-z\s(),[\]0-9]*$/) !== null;
  }

  sqlSelectAliasAsStruct(
    alias: string,
    dialectFieldList: DialectFieldList
  ): string {
    return `STRUCT(${dialectFieldList
      .map(d => `${alias}.${d.sqlOutputName} AS ${d.sqlOutputName}`)
      .join(', ')})`;
  }

  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE (${sample.rows} ROWS))`;
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE (${sample.percent} PERCENT))`;
      }
    }
    return tableSQL;
  }

  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    // (conv(substr(md5('hello'), 1, 15), 16, 10)::decimal(38, 0) * 4294967296 + conv(substr(md5('hello'), 16, 8), 16, 10)::decimal(38, 0)) *  0.000000001
    sqlDistinctKey = `${sqlDistinctKey}::STRING`;
    const upperPart = `conv(substr(md5_hex(${sqlDistinctKey}), 1, 15), 16, 10)::decimal(38, 0) * 4294967296`;
    const lowerPart = `conv(substr(md5_hex(${sqlDistinctKey}), 16, 8), 16, 10)::decimal(38, 0)`;

    const precisionShiftMultiplier = '0.000000001';
    return `(${upperPart} + ${lowerPart}) * ${precisionShiftMultiplier}`;
  }

  sqlCast(qi: QueryInfo, cast: TypecastFragment): Expr {
    if (cast.srcType === cast.dstType) {
      return cast.expr;
    }

    const op = `${cast.srcType}::${cast.dstType}`;
    const tz = qtz(qi);

    const maybeTimeExpr =
      tz === undefined
        ? cast.expr
        : mkExpr`convert_timezone('${tz}', (${cast.expr})::timestamp_ntz)`;
    if (op === 'timestamp::date') {
      return mkExpr`cast(${maybeTimeExpr} as date)`;
    } else if (op === 'date::timestamp') {
      return maybeTimeExpr;
    }
    const dstType =
      typeof cast.dstType === 'string'
        ? this.malloyTypeToSQLType({type: cast.dstType})
        : cast.dstType.raw;
    const castFunc = cast.safe ? 'try_cast' : 'cast';
    return mkExpr`${castFunc}(${cast.expr} as ${dstType})`;
  }

  sqlFieldReference(
    alias: string,
    fieldName: string,
    _fieldType: string,
    _isNested: boolean,
    _isArray: boolean
  ): string {
    // NOTE: how to support nested struct & array?
    return `${alias}.${this.sqlMaybeQuoteIdentifier(fieldName)}`;
  }

  sqlUnnestAlias(
    source: string,
    alias: string,
    _fieldList: DialectFieldList,
    _needDistinctKey: boolean,
    isArray: boolean,
    _isInNestedPipeline: boolean
  ): string {
    // TODO: support needDistinctKey
    if (isArray) {
      return `, LATERAL (select col as value from explode(${source})) as ${alias}`;
    } else {
      return `, LATERAL inline(${source}) as ${alias}`;
    }
  }

  sqlUnnestPipelineHead(
    isSingleton: boolean,
    sourceSQLExpression: string
  ): string {
    let p = sourceSQLExpression;
    if (isSingleton) {
      p = `ARRAY(${p})`;
    }
    return `EXPLODE(${p})`;
  }

  sqlLiteralTime(
    qi: QueryInfo,
    timeString: string,
    type: TimeFieldType,
    timezone: string | undefined
  ): string {
    if (type === 'date') {
      return `DATE '${timeString}'`;
    }
    const tz = timezone || qtz(qi);
    if (tz) {
      return `TIMESTAMP '${timeString} ${tz}'`;
    }
    return `TIMESTAMP '${timeString}'`;
  }

  sqlAlterTime(
    op: '+' | '-',
    expr: TimeValue,
    n: Expr,
    timeframe: DateUnit
  ): Expr {
    // TODO: does not support quarter
    const interval = mkExpr`INTERVAL '${n} ${timeframe}'`;
    return mkExpr`((${expr.value}) ${op} ${interval})`;
  }

  sqlExtract(qi: QueryInfo, from: TimeValue, units: ExtractUnit): Expr {
    const extractUnits = extractionMap[units] || units;
    let extractFrom = from.value;

    const tz = qtz(qi);
    if (tz && from.valueType === 'timestamp') {
      extractFrom = mkExpr`CONVERT_TIMEZONE('${tz}', ${extractFrom})`;
    }
    const extracted = mkExpr`EXTRACT(${extractUnits} FROM ${extractFrom})`;
    return extracted;
  }

  sqlTrunc(qi: QueryInfo, sqlTime: TimeValue, units: TimestampUnit): Expr {
    const tz = qtz(qi);
    let truncThis = sqlTime.value;
    if (tz && sqlTime.valueType === 'timestamp') {
      truncThis = mkExpr`CONVERT_TIMEZONE('${tz}', ${truncThis})`;
    }
    return mkExpr`DATE_TRUNC('${units}', ${truncThis})`;
  }

  sqlMeasureTime(from: TimeValue, to: TimeValue, units: string): Expr {
    type TimeMeasure = {use: string; ratio: number};
    type secondsUnits = 'microsecond' | 'millisecond' | 'second';
    const measureFunction: Record<secondsUnits, string> = {
      'microsecond': 'unix_micros',
      'millisecond': 'unix_millis',
      'second': 'unix_seconds',
    };
    const measureMap: Record<string, TimeMeasure> = {
      'microsecond': {use: 'microsecond', ratio: 1},
      'millisecond': {use: 'microsecond', ratio: 1000},
      'second': {use: 'millisecond', ratio: 1000},
      'minute': {use: 'second', ratio: 60},
      'hour': {use: 'second', ratio: 3660},
      'day': {use: 'day', ratio: 1},
    };
    let lVal = from.value;
    let rVal = to.value;
    if (measureMap[units]) {
      const {use: measureIn, ratio} = measureMap[units];
      if (allUnits.indexOf(units) > dayIndex) {
        throw new Error(`Measure in '${measureIn}' not implemented`);
      }
      if (from.valueType !== to.valueType) {
        throw new Error("Can't measure difference between different types");
      }
      if (from.valueType === 'date') {
        lVal = mkExpr`TIMESTAMP(${lVal})`;
        rVal = mkExpr`TIMESTAMP(${rVal})`;
      }

      let measured: Expr | undefined;
      switch (measureIn) {
        case 'microsecond' || 'millisecond' || 'second':
          measured = mkExpr`${measureFunction[measureIn]}(${rVal}) - ${measureFunction[measureIn]}(${lVal})`;
          break;
        case 'day':
          measured = mkExpr`DATE_DIFF(${rVal}, ${lVal})`;
          break;
        default:
          throw new Error(`measureIn '${measureIn} not implemented`);
      }

      if (ratio !== 1) {
        measured = mkExpr`FLOOR(${measured}/${ratio.toString()}.0)`;
      }
      return measured;
    }
    throw new Error(`Measure '${units} not implemented`);
  }

  concat(...values: string[]): string {
    return values.join(' || ');
  }
}
