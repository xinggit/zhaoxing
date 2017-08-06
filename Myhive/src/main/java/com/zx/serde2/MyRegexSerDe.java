/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zx.serde2;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * RegexSerDe uses regular expression (regex) to deserialize data. It doesn't
 * support data serialization.
 *
 * It can deserialize the data using regex and extracts groups as columns.
 *
 * In deserialization stage, if a row does not match the regex, then all columns
 * in the row will be NULL. If a row matches the regex but has less than
 * expected groups, the missing groups will be NULL. If a row matches the regex
 * but has more than expected groups, the additional groups are just ignored.
 *
 * NOTE: Regex SerDe supports primitive column types such as TINYINT, SMALLINT,
 * INT, BIGINT, FLOAT, DOUBLE, STRING, BOOLEAN and DECIMAL
 *
 *
 * NOTE: This implementation uses javaStringObjectInspector for STRING. A more
 * efficient implementation should use UTF-8 encoded Text and
 * writableStringObjectInspector. We should switch to that when we have a UTF-8
 * based Regex library.
 */
public class MyRegexSerDe extends AbstractSerDe {

	public static final Logger LOG = LoggerFactory.getLogger(MyRegexSerDe.class.getName());

	public static final String INPUT_REGEX = "input.regex";
	public static final String INPUT_REGEX_CASE_SENSITIVE = "input.regex.case.insensitive";

	private static final String LIST_DELIMITED = ",";
	private static final String MAP_DELIMITED = "=";
	private static final String STRUCT_DELIMITED = ",";

	int numColumns;
	String inputRegex;

	Pattern inputPattern;

	StructObjectInspector rowOI;
	List<Object> row;
	List<TypeInfo> columnTypes;
	Object[] outputFields;
	Text outputRowText;

	boolean alreadyLoggedNoMatch = false;
	boolean alreadyLoggedPartialMatch = false;

	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {

		// We can get the table definition from tbl.

		// Read the configuration parameters
		inputRegex = tbl.getProperty(INPUT_REGEX);
		// 得到列名
		String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		// 得到列类型
		String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

		// input.regex.case.insensitive：是否忽略字母大小写，默认为false
		boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(tbl.getProperty(INPUT_REGEX_CASE_SENSITIVE));

		// output format string is not supported anymore, warn user of
		// deprecation 输出正则表达式被弃用
		if (null != tbl.getProperty("output.format.string")) {
			LOG.warn("output.format.string has been deprecated");
		}

		// Parse the configuration parameters采用何在方式编写，以及是否忽略大小写
		if (inputRegex != null) {
			inputPattern = Pattern.compile(inputRegex,
					Pattern.DOTALL + (inputRegexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
		} else {
			inputPattern = null;
			throw new SerDeException("This table does not have serde property \"input.regex\"!");
		}

		// 得到列名分隔符,默认为 ,
		final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER)
				? tbl.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
		List<String> columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
		columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		assert columnNames.size() == columnTypes.size();
		numColumns = columnNames.size();

		/*
		 * Constructing the row ObjectInspector: The row consists of some set of
		 * primitive columns, each column will be a java object of primitive
		 * type.
		 */// 对象检查员
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
		for (int c = 0; c < numColumns; c++) {
			TypeInfo typeInfo = columnTypes.get(c);
			// 判断类型是否为原始类型--->具体点进去看父类
			if (typeInfo instanceof PrimitiveTypeInfo) {
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);
				AbstractPrimitiveJavaObjectInspector oi = PrimitiveObjectInspectorFactory
						.getPrimitiveJavaObjectInspector(pti);
				columnOIs.add(oi);
			} else {
				TypeInfo pti = columnTypes.get(c);
				columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti));
				// throw new SerDeException(getClass().getName() + " doesn't
				// allow column [" + c + "] named "
				// + columnNames.get(c) + " with type " + columnTypes.get(c));
			}
		}

		// StandardStruct uses ArrayList to store the row.
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs,
				Lists.newArrayList(Splitter.on('\0').split(tbl.getProperty("columns.comments"))));

		row = new ArrayList<Object>(numColumns);
		// Constructing the row object, etc, which will be reused for all rows.
		for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}
		outputFields = new Object[numColumns];
		outputRowText = new Text();
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	// Number of rows not matching the regex
	long unmatchedRowsCount = 0;
	// Number of rows that match the regex but have missing groups.
	long partialMatchedRowsCount = 0;

	@Override
	public Object deserialize(Writable blob) throws SerDeException {

		Text rowText = (Text) blob;
		Matcher m = inputPattern.matcher(rowText.toString());

		if (m.groupCount() != numColumns) {
			throw new SerDeException("Number of matching groups doesn't match the number of columns");
		}

		// If do not match, ignore the line, return a row with all nulls.
		// 不匹配执行下面if语句
		if (!m.matches()) {
			// Log.info("===========no match");
			unmatchedRowsCount++;
			if (!alreadyLoggedNoMatch) {
				// Report the row if its the first time
				LOG.warn("" + unmatchedRowsCount + " unmatched rows are found: " + rowText);
				alreadyLoggedNoMatch = true;
			}
			return null;
		}

		// Otherwise, return the row.
		for (int c = 0; c < numColumns; c++) {
			try {
				// t=1列的值
				String t = m.group(c + 1);// 从1开始
				TypeInfo typeInfo = columnTypes.get(c);// 的到表的类型

				// Convert the column to the correct type when needed and set in
				// row obj

				if (typeInfo instanceof PrimitiveTypeInfo) {
					PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
					switch (pti.getPrimitiveCategory()) {
					case STRING:
						row.set(c, t);
						break;
					case BYTE:
						Byte b;
						b = Byte.valueOf(t);
						row.set(c, b);
						break;
					case SHORT:
						Short s;
						s = Short.valueOf(t);
						row.set(c, s);
						break;
					case INT:
						Integer i;
						i = Integer.valueOf(t);
						row.set(c, i);
						break;
					case LONG:
						Long l;
						l = Long.valueOf(t);
						row.set(c, l);
						break;
					case FLOAT:
						Float f;
						f = Float.valueOf(t);
						row.set(c, f);
						break;
					case DOUBLE:
						Double d;
						d = Double.valueOf(t);
						row.set(c, d);
						break;
					case BOOLEAN:
						Boolean bool;
						bool = Boolean.valueOf(t);
						row.set(c, bool);
						break;
					case TIMESTAMP:
						Timestamp ts;
						ts = Timestamp.valueOf(t);
						row.set(c, ts);
						break;
					case DATE:
						Date date;
						date = Date.valueOf(t);
						row.set(c, date);
						break;
					case DECIMAL:
						HiveDecimal bd = HiveDecimal.create(t);
						row.set(c, bd);
						break;
					case CHAR:
						HiveChar hc = new HiveChar(t, ((CharTypeInfo) typeInfo).getLength());
						row.set(c, hc);
						break;
					case VARCHAR:
						HiveVarchar hv = new HiveVarchar(t, ((VarcharTypeInfo) typeInfo).getLength());
						row.set(c, hv);
						break;
					default:
						break;
					// throw new SerDeException("Unsupported type " + typeInfo);
					}
				}
				switch (typeInfo.getCategory()) {
				case LIST:
					List<String> list = new ArrayList<>();
					String[] val = t.split(LIST_DELIMITED);
					for (String string : val) {
						list.add(string);
					}
					row.set(c, list);
					break;
				case MAP:
					Map<String, String> map = new HashMap<String, String>();
					String[] mapval = t.split(LIST_DELIMITED);
					for (String value : mapval) {
						String[] kv = value.split(MAP_DELIMITED);
						if (kv.length == 2) {
							map.put(kv[0], kv[1]);
						}
					}
					row.set(c, map);
					break;
				case STRUCT:
					List<Object> struct = new ArrayList<>();
					String[] structval = t.split(STRUCT_DELIMITED);
					for (String value : structval) {
						struct.add(value);
					}
					row.set(c, struct);
					break;
				default:
					break;
				}

			} catch (RuntimeException e) {
				partialMatchedRowsCount++;
				if (!alreadyLoggedPartialMatch) {
					// Report the row if its the first row
					LOG.warn("" + partialMatchedRowsCount + " partially unmatched rows are found, "
							+ " cannot find group " + c + ": " + rowText);
					alreadyLoggedPartialMatch = true;
				}
				row.set(c, null);
			}
		}
		return row;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
		throw new UnsupportedOperationException("Regex SerDe doesn't support the serialize() method");
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no support for statistics
		return null;
	}
}
